"""Cloud opt-in, private destinations, metrics privacy and mandatory HTTPS authentication."""
import base64
import unittest
from unittest.mock import patch
from flask import Flask
from werkzeug.security import generate_password_hash
from common.deployment import require_profile,service_endpoint,zk_hosts
from common.observability import install
from common.web_access import CloudAccess

ENV={"APP_ENV":"cloud","CLOUD_DEPLOYMENT_ACK":"assignment-private-network-v1","PROJECT_ID":"assignment-test-123","ZK_HOSTS":"zk-0:2181,zk-1:2181,zk-2:2181","ZK_ALLOWED_HOSTS":"zk-0:2181,zk-1:2181,zk-2:2181"}
class Profiles(unittest.TestCase):
    def test_unknown_or_unacknowledged_mode_fails_closed(self):
        for env in ({},{"APP_ENV":"production"},{"APP_ENV":"cloud"}):
            with self.assertRaises(RuntimeError):require_profile(environ=env)
    def test_cloud_rejects_emulator_and_host_credential_file(self):
        self.assertEqual(require_profile(environ=ENV),"cloud")
        for key in ("FIRESTORE_EMULATOR_HOST","GOOGLE_APPLICATION_CREDENTIALS"):
            with self.assertRaises(RuntimeError):require_profile(environ={**ENV,key:"not-a-cloud-identity"})
    def test_private_endpoint_and_coordination_allowlists(self):
        self.assertEqual(service_endpoint("http://customer-service:8083",set(),environ=ENV),"http://customer-service:8083")
        self.assertEqual(zk_hosts(ENV),ENV["ZK_HOSTS"])
        for value in ("https://external.example:443","http://customer-service:8083/private","http://user:password@customer-service:8083","http://169.254.169.254:80"):
            with self.assertRaises(RuntimeError):service_endpoint(value,set(),environ=ENV)
        with self.assertRaises(RuntimeError):zk_hosts({**ENV,"ZK_HOSTS":"other:2181"})
    def test_metrics_use_route_templates_without_private_values(self):
        app=Flask(__name__);app.testing=True;install(app,"test")
        app.add_url_rule("/customers/<value>",view_func=lambda value:("",503))
        browser=app.test_client();browser.get("/customers/private-identity?credential=synthetic-private-marker")
        text=browser.get("/metrics").get_data(as_text=True)
        self.assertIn('route="/customers/<value>"',text);self.assertIn('status="503"',text)
        self.assertNotIn("private-identity",text);self.assertNotIn("synthetic-private-marker",text)
    def test_cloud_web_requires_secure_authenticated_canonical_origin(self):
        env={"WEB_PUBLIC_ORIGIN":"https://food.example.test","WEB_OPERATOR_USER":"operator","WEB_OPERATOR_PASSWORD_HASH":generate_password_hash("synthetic-test-passphrase")}
        access=CloudAccess(env);app=Flask(__name__);app.before_request(access.authorize)
        app.add_url_rule("/","home",view_func=lambda:"ok");app.add_url_rule("/health","health",view_func=lambda:"healthy")
        browser=app.test_client();header={"Authorization":"Basic "+base64.b64encode(b"operator:synthetic-test-passphrase").decode()}
        self.assertEqual(browser.get("/",base_url="https://food.example.test").status_code,401)
        self.assertEqual(browser.get("/",base_url="http://food.example.test",headers=header).status_code,403)
        self.assertEqual(browser.get("/",base_url="https://other.example.test",headers=header).status_code,400)
        self.assertEqual(browser.get("/",base_url="https://food.example.test",headers=header).status_code,200)
        self.assertEqual(browser.get("/health",base_url="http://127.0.0.1").status_code,200)
    def test_cloud_web_cannot_start_without_operator_configuration(self):
        with self.assertRaises(RuntimeError):CloudAccess({"WEB_PUBLIC_ORIGIN":"https://food.example.test"})
if __name__=="__main__":unittest.main()
