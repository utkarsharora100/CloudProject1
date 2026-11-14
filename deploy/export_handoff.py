"""Export a strict source allowlist; never includes Git history, state, local data or real configuration."""
import hashlib
import json
from pathlib import Path
import re
import zipfile
ROOT=Path(__file__).resolve().parents[1]
SOURCE_DIRS={"common","customer_service","order_service","task_scheduler","payment_service","kitchen_workflow","delivery_assignment_service","notification_service","restaurant_service","web_ui","local","tests","deploy","functions","observability","load","docs"}
EXTENSIONS={".py",".js",".mjs",".json",".yaml",".yml",".html",".css",".svg",".proto",".java",".xml",".properties",".md",".hcl",".conf"}
ROOT_FILES={"requirements.txt","compose.yaml","compose.assignment.yaml","Dockerfile.local","Dockerfile.local.dockerignore",".env.local.example",".env.assignment.example",".gitignore"}
def candidates():
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or path.is_symlink():continue
        relative=path.relative_to(ROOT);parts=relative.parts
        if any(part in {".git",".local-build",".terraform","__pycache__","venv",".venv","target","node_modules","team"} for part in parts):continue
        if len(parts)==1:
            if path.name in ROOT_FILES or path.suffix==".md":yield path
            continue
        if parts[0] not in SOURCE_DIRS:continue
        if path.name=="config.team.json" or "tfstate" in path.name.lower() or ".tfvars" in path.name.lower() or "backend.hcl"==path.name:continue
        if path.name.startswith(".env"):continue
        if path.name.startswith("Dockerfile") and parts[0] not in {"functions"} and parts[:2]!=("restaurant_service","analytics"):continue
        if path.suffix in EXTENSIONS or path.name.startswith("Dockerfile") or path.name in {".dockerignore","requirements.txt","Chart.lock"} or (parts[:3]==("deploy","charts","platform") and path.suffix==".tgz"):
            # Legacy deployment files are superseded by deploy/, never exported.
            if path.suffix==".tf" or (parts[0] in {"customer_service","order_service","restaurant_service","delivery_assignment_service"} and path.suffix in {".yaml",".yml"}):continue
            yield path
def known_private_values():
    values=set()
    for path in [ROOT/"secrets.txt",ROOT/"secret.txt",ROOT/"customer_service/terraform.tfstate.backup"]:
        if not path.exists():continue
        raw=path.read_text(encoding="utf-8",errors="ignore")
        # Existing exposure inventory stays in memory; never display source/value.
        for match in re.finditer(r'(?im)["\x27]?(?:[A-Z_]*(?:PASSWORD|SECRET|API_KEY|API_SECRET)|password|api_key|api_secret)["\x27]?\s*[:=]\s*["\x27]?([^\s"\x27,}]+)',raw):
            if len(match.group(1))>=8:values.add(match.group(1).encode())
    return values
def verify(files,known=()):
    for relative,data in files:
        if any(value in data for value in known):raise ValueError("Known exposed credential match: "+relative)
        if (b"-----BEGIN "+b"PRIVATE KEY-----") in data or (b"-----BEGIN "+b"RSA PRIVATE KEY-----") in data:raise ValueError("Private key material: "+relative)
        if re.search(rb"AIza[A-Za-z0-9_-]{35}",data):raise ValueError("Google API key pattern: "+relative)
        if "secrets.auto.tfvars" in relative or ".git/" in relative or "tfstate" in relative or "config.team.json" in relative:raise ValueError("Private path: "+relative)
def main():
    files=[(p.relative_to(ROOT).as_posix(),p.read_bytes()) for p in candidates()]
    known=known_private_values()
    try:verify(files,known)
    except ValueError as error:raise SystemExit(str(error)+" [value redacted]")
    output=ROOT/".local-build/handoff";output.mkdir(parents=True,exist_ok=True)
    target=output/"food-delivery-team-handoff.zip"
    manifest={name:hashlib.sha256(data).hexdigest() for name,data in files}
    with zipfile.ZipFile(target,"w",zipfile.ZIP_DEFLATED) as archive:
        for name,data in files:archive.writestr(name,data)
        archive.writestr("HANDOFF_MANIFEST.json",json.dumps(manifest,indent=2)+"\n")
    with zipfile.ZipFile(target) as archive:verify([(name,archive.read(name)) for name in archive.namelist()],known)
    checksum=hashlib.sha256(target.read_bytes()).hexdigest()
    target.with_suffix(".zip.sha256").write_text(checksum+"  "+target.name+"\n")
    print(json.dumps({"artifact":str(target),"source_files":len(files),"sha256":checksum,"known_exposure_matches":0,"git_history_included":False,"local_state_or_credentials_included":False}))
if __name__=="__main__":main()
