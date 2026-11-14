"""Interactive, non-echoed secret input; writes ignored Terraform inputs, never prints values."""
import argparse
import getpass
import json
import os
from pathlib import Path
from werkzeug.security import generate_password_hash
ROOT=Path(__file__).resolve().parents[1]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("scope",choices=["cloud","kafka"]);args=parser.parse_args()
    target=ROOT/"deploy/terraform"/args.scope/"secrets.auto.tfvars.json"
    if target.exists():raise SystemExit("Secret input already exists. Review/remove the ignored file deliberately before replacing it.")
    print("Use a newly rotated key. Do not paste any value into chat or a command argument.")
    key=getpass.getpass("Kafka "+("application" if args.scope=="cloud" else "topic administrator")+" API key: ")
    secret=getpass.getpass("Kafka API secret: ")
    if not key or not secret:raise SystemExit("Empty secret input rejected.")
    values={"kafka_api_key":key,"kafka_api_secret":secret}
    if args.scope=="cloud":
        password=getpass.getpass("HTTPS demo operator passphrase (16+ characters): ")
        confirmation=getpass.getpass("Repeat operator passphrase: ")
        if password!=confirmation or len(password)<16:raise SystemExit("Passphrase confirmation/length rejected.")
        values["operator_password_hash"]=generate_password_hash(password)
    target.parent.mkdir(parents=True,exist_ok=True)
    descriptor=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(descriptor,"w",encoding="utf-8") as stream:json.dump(values,stream)
    print("Ignored input file created. Restrict its Windows ACL to your user; Terraform state also contains secret data.")
if __name__=="__main__":main()
