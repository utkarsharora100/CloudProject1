package org.fooddelivery.analytics;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.*;
import java.io.Serializable;
import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.*;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Restaurant Service: keyed event-time minute windows and checkpointed Kafka results. */
public final class RestaurantAnalytics {
 static final Gson JSON=new Gson();
 static final Map<String,String> OPTIONS=new HashMap<>();
 public static final class Event implements Serializable {
  public String eventId,orderId,restaurantId,status; public long timestamp; public Event(){}
 }
 public static Event parse(String raw,long now){
  if(raw==null||raw.length()>65536)throw new IllegalArgumentException("event_size");
  JsonObject v=JsonParser.parseString(raw).getAsJsonObject();
  if(!v.get("schema_version").isJsonPrimitive()||!v.get("schema_version").getAsJsonPrimitive().isNumber()||!v.get("schema_version").toString().equals("2"))throw new IllegalArgumentException("event_schema");
  Event e=new Event();e.eventId=UUID.fromString(v.get("event_id").getAsString()).toString();
  e.orderId=UUID.fromString(v.get("order_id").getAsString()).toString();
  e.restaurantId=v.get("restaurant_id").getAsString();e.status=v.get("status").getAsString();
  if(!e.restaurantId.matches("[A-Za-z0-9][A-Za-z0-9_-]{0,63}")||!Set.of("PENDING_PAYMENT","PAYMENT_FAILED","PAID","KITCHEN_ACCEPTED","PREPARING","READY","COURIER_ASSIGNED","PICKED_UP","DELIVERED","CANCELLED","REFUND_PENDING","REFUNDED").contains(e.status))throw new IllegalArgumentException("event_identity");
  if(!e.eventId.equals(v.get("event_id").getAsString())||!e.orderId.equals(v.get("order_id").getAsString()))throw new IllegalArgumentException("event_uuid");
  e.timestamp=Instant.parse(v.get("timestamp").getAsString()).toEpochMilli();
  if(e.timestamp<0||e.timestamp>now+120000)throw new IllegalArgumentException("event_time");return e;
 }
 public static long windowStart(long timestamp,long interval){return Math.floorDiv(timestamp,interval)*interval;}
 public static final class Parse extends RichFlatMapFunction<String,Event>{
  private transient Counter rejected;
  @Override public void open(OpenContext context){rejected=getRuntimeContext().getMetricGroup().counter("events_rejected");}
  @Override public void flatMap(String raw,Collector<Event> out){
   try{out.collect(parse(raw,System.currentTimeMillis()));}catch(RuntimeException invalid){rejected.inc();}
  }
 }
 public static final class Windows extends KeyedProcessFunction<String,Event,String>{
  private final long interval;
  private transient MapState<String,Boolean> events,orders;
  private transient MapState<String,Long> stages;
  private transient ValueState<Long> eventCount,orderCount;
  private transient Counter late;
  public Windows(long interval){this.interval=interval;}
  @Override public void open(OpenContext context)throws Exception{
   events=getRuntimeContext().getMapState(new MapStateDescriptor<>("event-identities",String.class,Boolean.class));
   orders=getRuntimeContext().getMapState(new MapStateDescriptor<>("order-identities",String.class,Boolean.class));
   stages=getRuntimeContext().getMapState(new MapStateDescriptor<>("stage-counts",String.class,Long.class));
   eventCount=getRuntimeContext().getState(new ValueStateDescriptor<>("event-count",Long.class));
   orderCount=getRuntimeContext().getState(new ValueStateDescriptor<>("order-count",Long.class));
   late=getRuntimeContext().getMetricGroup().counter("events_late");
  }
  @Override public void processElement(Event e,Context context,Collector<String> out)throws Exception{
   long end=windowStart(e.timestamp,interval)+interval;
   if(end-1<=context.timerService().currentWatermark()){late.inc();return;}
   if(events.contains(e.eventId))return;events.put(e.eventId,true);
   eventCount.update(eventCount.value()==null?1:eventCount.value()+1);
   if(!orders.contains(e.orderId)){orders.put(e.orderId,true);orderCount.update(orderCount.value()==null?1:orderCount.value()+1);}
   Long count=stages.get(e.status);stages.put(e.status,count==null?1:count+1);context.timerService().registerEventTimeTimer(end-1);
  }
  @Override public void onTimer(long timestamp,OnTimerContext context,Collector<String> out)throws Exception{
   String key=context.getCurrentKey();int split=key.lastIndexOf('|');long start=Long.parseLong(key.substring(split+1));
   Map<String,Object> value=new LinkedHashMap<>();value.put("schema_version",1);value.put("source","restaurant-flink-event-time-v1");
   value.put("restaurant_id",key.substring(0,split));value.put("window_start",Instant.ofEpochMilli(start).toString());
   value.put("window_end",Instant.ofEpochMilli(start+interval).toString());value.put("window_seconds",interval/1000);
   value.put("unique_order_count",orderCount.value());value.put("event_count",eventCount.value());
   Map<String,Long> counts=new TreeMap<>();for(Map.Entry<String,Long> item:stages.entries())counts.put(item.getKey(),item.getValue());
   value.put("stage_counts",counts);out.collect(JSON.toJson(value));
   events.clear();orders.clear();stages.clear();eventCount.clear();orderCount.clear();
  }
 }
 static String env(String key,String fallback){String value=OPTIONS.getOrDefault(key,System.getenv(key));return value==null||value.isBlank()?fallback:value;}
 static Properties kafka()throws Exception{
  Properties p=new Properties();p.setProperty("bootstrap.servers",env("KAFKA_BOOTSTRAP_SERVERS","kafka:9092"));
  if(env("APP_ENV","").equals("local")){
   if(!env("KAFKA_SECURITY_PROTOCOL","PLAINTEXT").equals("PLAINTEXT"))throw new IllegalArgumentException("Local Kafka mismatch");
   p.setProperty("security.protocol","PLAINTEXT");
  }else{
   if(!env("APP_ENV","").equals("cloud"))throw new IllegalArgumentException("Explicit profile required");
   String version=env("KAFKA_SECRET_VERSION","");
   if(!version.matches("projects/[a-z0-9-]+/secrets/[a-zA-Z0-9_-]+/versions/[0-9]+"))throw new IllegalArgumentException("Pinned secret version required");
   GoogleCredentials credential=GoogleCredentials.getApplicationDefault().createScoped("https://www.googleapis.com/auth/cloud-platform");credential.refreshIfExpired();
   HttpRequest request=HttpRequest.newBuilder(URI.create("https://secretmanager.googleapis.com/v1/"+version+":access")).timeout(Duration.ofSeconds(10))
    .header("Authorization","Bearer "+credential.getAccessToken().getTokenValue()).GET().build();
   HttpResponse<String> response=HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.ofString());
   if(response.statusCode()!=200)throw new IllegalStateException("Kafka secret access failed");
   JsonObject envelope=JsonParser.parseString(response.body()).getAsJsonObject();
   JsonObject secret=JsonParser.parseString(new String(Base64.getDecoder().decode(envelope.getAsJsonObject("payload").get("data").getAsString()),StandardCharsets.UTF_8)).getAsJsonObject();
   String user=secret.get("api_key").getAsString(),password=secret.get("api_secret").getAsString();
   if(!user.matches("[A-Za-z0-9_-]+")||!password.matches("[A-Za-z0-9+/=_-]+"))throw new IllegalArgumentException("Credential encoding unsupported");
   p.setProperty("security.protocol","SASL_SSL");p.setProperty("sasl.mechanism","PLAIN");
   p.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\""+user+"\" password=\""+password+"\";");
  }return p;
 }
 public static void main(String[] args)throws Exception{
  if(args.length>1 || (args.length==1 && !args[0].startsWith("--settings-json=")))throw new IllegalArgumentException("Use a single non-secret settings JSON argument.");
  if(args.length==1){
   JsonObject config=JsonParser.parseString(args[0].substring(16)).getAsJsonObject();
   Set<String> allowed=Set.of("APP_ENV","KAFKA_BOOTSTRAP_SERVERS","KAFKA_SECRET_VERSION","ANALYTICS_WINDOW_SECONDS","ANALYTICS_CHECKPOINT_URI","ANALYTICS_INPUT_TOPIC","ANALYTICS_RESULTS_TOPIC","ANALYTICS_GROUP","ANALYTICS_TRANSACTION_PREFIX");
   for(Map.Entry<String,JsonElement> item:config.entrySet()){
    if(!allowed.contains(item.getKey())||!item.getValue().isJsonPrimitive()||!item.getValue().getAsJsonPrimitive().isString())throw new IllegalArgumentException("Unsupported non-secret settings field.");
    OPTIONS.put(item.getKey(),item.getValue().getAsString());
   }
  }
  long seconds=Long.parseLong(env("ANALYTICS_WINDOW_SECONDS","60"));
  if(seconds<5||seconds>3600)throw new IllegalArgumentException("Window must be 5-3600 seconds");
  long interval=seconds*1000;Properties settings=kafka();
  settings.setProperty("transaction.timeout.ms","600000");
  Configuration configuration=new Configuration();
  configuration.setString("execution.checkpointing.externalized-checkpoint-retention","RETAIN_ON_CANCELLATION");
  configuration.setString("execution.checkpointing.storage","filesystem");
  configuration.setString("execution.checkpointing.dir",env("ANALYTICS_CHECKPOINT_URI","file:///opt/flink/checkpoints"));
  StreamExecutionEnvironment execution=StreamExecutionEnvironment.getExecutionEnvironment(configuration);
  execution.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE);
  execution.getCheckpointConfig().setCheckpointTimeout(120000);
  KafkaSource<String> source=KafkaSource.<String>builder().setProperties(settings).setTopics(env("ANALYTICS_INPUT_TOPIC","orders"))
   .setGroupId(env("ANALYTICS_GROUP","restaurant-analytics-v1")).setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
   .setValueOnlyDeserializer(new SimpleStringSchema()).build();
  KafkaSink<String> sink=KafkaSink.<String>builder().setKafkaProducerConfig(settings)
   .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder().setTopic(env("ANALYTICS_RESULTS_TOPIC","restaurant_analytics_results")).setValueSerializationSchema(new SimpleStringSchema()).build())
   .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE).setTransactionalIdPrefix(env("ANALYTICS_TRANSACTION_PREFIX","restaurant-analytics-v1-")).build();
  execution.fromSource(source,WatermarkStrategy.noWatermarks(),"Kafka order events").uid("orders-source")
   .flatMap(new Parse()).returns(Event.class).name("Validated private-data-free events").uid("event-parser")
   .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner((e,previous)->e.timestamp).withIdleness(Duration.ofSeconds(10)))
   .keyBy(e->e.restaurantId+"|"+windowStart(e.timestamp,interval)).process(new Windows(interval))
   .name("Unique active orders per restaurant minute").uid("restaurant-minute-windows")
   .sinkTo(sink).name("Committed Kafka window results").uid("results-sink");
  execution.execute("Restaurant Service - stateful kitchen-load analytics");
 }
}
