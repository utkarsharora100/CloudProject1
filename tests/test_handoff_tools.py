import copy
import json
from pathlib import Path
import unittest
from deploy.configure import validate,render
from deploy.dataproc_submit import spec,submit
from deploy.process_runner import redact
ROOT=Path(__file__).resolve().parents[1]
ENV={"PROJECT_ID":"assignment-test-123","REGION":"asia-south1","DATAPROC_CLUSTER":"food-flink","KAFKA_BOOTSTRAP_SERVERS":"broker.example:9092","KAFKA_SECRET_VERSION":"projects/assignment-test-123/secrets/food-kafka/versions/1","ANALYTICS_CHECKPOINT_URI":"gs://protected-checkpoints/flink","ANALYTICS_JAR_URI":"gs://protected-artifacts/restaurant-release.jar","ANALYTICS_RELEASE":"release1"}
class Response:
 def __init__(self,status,value):self.status_code=status;self.value=value
 def json(self):return self.value
class Session:
 def __init__(self,responses):self.responses=iter(responses);self.posts=[]
 def get(self,*args,**kwargs):return next(self.responses)
 def post(self,*args,**kwargs):self.posts.append(kwargs["json"]);return Response(200,{})
class Handoff(unittest.TestCase):
 def test_redaction_handles_db_urls_keys_and_longer_overlapping_values(self):
  text=redact("mysql+pymysql://account:synthetic-private-value@mysql/db synthetic-private-value-more",["synthetic-private-value","synthetic-private-value-more"])
  self.assertNotIn("synthetic-private",text);self.assertNotIn("account:",text)
 def test_managed_job_identity_is_stable_without_secrets_in_arguments(self):
  first=spec(ENV)[2];self.assertEqual(first,spec(dict(ENV))[2])
  changed=spec({**ENV,"ANALYTICS_RELEASE":"release2"})[2]
  self.assertNotEqual(first["reference"],changed["reference"])
  settings=json.loads(first["flinkJob"]["args"][0].split("=",1)[1])
  self.assertEqual(settings["KAFKA_SECRET_VERSION"],ENV["KAFKA_SECRET_VERSION"])
  self.assertNotIn("api_secret",settings)
  self.assertEqual(spec({**ENV,"ANALYTICS_SAVEPOINT":"gs://protected-checkpoints/savepoint"})[2]["flinkJob"]["savepointUri"],"gs://protected-checkpoints/savepoint")
 def test_managed_launch_reuses_existing_active_release(self):
  session=Session([Response(200,{"status":{"state":"RUNNING"}})])
  self.assertEqual(submit(session,ENV)[1],"existing");self.assertEqual(session.posts,[])
 def test_managed_launch_refuses_parallel_releases_and_terminal_reuse(self):
  for rows in [[Response(200,{"status":{"state":"ERROR"}})],[Response(404,{}),Response(200,{"jobs":[{"status":{"state":"RUNNING"}}]})]]:
   with self.assertRaises(RuntimeError):submit(Session(rows),ENV)
 def test_managed_launch_records_only_nonsecret_job_spec(self):
  session=Session([Response(404,{}),Response(200,{"jobs":[]})])
  self.assertEqual(submit(session,ENV)[1],"submitted")
  self.assertEqual(session.posts[0]["job"]["flinkJob"]["mainClass"],"org.fooddelivery.analytics.RestaurantAnalytics")
 def test_config_rejects_private_fields_and_unrestricted_controlplane(self):
  config=json.loads((ROOT/"deploy/config.example.json").read_text())
  self.assertEqual(validate(config,True),config)
  for changed in [{**config,"api_secret":"synthetic-private"},{**config,"admin_cidr":"0.0.0.0/0"},{**config,"git_repository":"https://user:password@example.test/repo"},{**config,"image_tag":"latest"}]:
   with self.assertRaises(ValueError):validate(changed,True)
  with self.assertRaises(ValueError):validate(config)
if __name__=="__main__":unittest.main()
