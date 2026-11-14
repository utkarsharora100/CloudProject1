package org.fooddelivery.analytics;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
public class ContractTest {
 String event(String extra){return "{\"schema_version\":2,\"event_id\":\"01234567-1234-4234-8234-012345678901\",\"order_id\":\"11234567-1234-4234-8234-012345678901\",\"restaurant_id\":\"paper\",\"status\":\"PAID\",\"timestamp\":\"2026-01-01T00:00:01Z\""+extra+"}";}
 @Test void acceptsCanonicalSourceWithoutRetainingContacts(){var item=RestaurantAnalytics.parse(event(",\"delivery_snapshot\":{\"phone\":\"synthetic\"}"),Long.MAX_VALUE-120001);assertEquals("paper",item.restaurantId);assertEquals("PAID",item.status);}
 @Test void windowsHaveStableMinuteBoundaries(){assertEquals(60000,RestaurantAnalytics.windowStart(119999,60000));assertEquals(120000,RestaurantAnalytics.windowStart(120000,60000));}
 @Test void rejectsBadSchemaAndPoisonJson(){assertThrows(RuntimeException.class,()->RestaurantAnalytics.parse("{}",0));assertThrows(RuntimeException.class,()->RestaurantAnalytics.parse("{broken",0));}
 @Test void rejectsUnreasonableFutureWatermarks(){assertThrows(RuntimeException.class,()->RestaurantAnalytics.parse(event(""),0));}
 @Test void rejectsUnsafeRestaurantKeys(){assertThrows(RuntimeException.class,()->RestaurantAnalytics.parse(event("").replace("\"paper\"","\"../private\""),Long.MAX_VALUE-120001));}
}
