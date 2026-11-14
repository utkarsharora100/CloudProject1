"""Resolve trusted CLI paths without routing values through a Windows shell."""
import os
from pathlib import Path
import shutil
import sys
def resolve(arguments,*,platform=None,which=None):
    executable=(which or shutil.which)(arguments[0])
    if not executable:raise RuntimeError("Install the required command-line tool: "+arguments[0])
    if (platform or os.name)=="nt" and Path(executable).suffix.lower() in {".cmd",".bat"}:
        if arguments[0]!="gcloud":raise RuntimeError("A native executable is required for this tool.")
        sdk=Path(executable).resolve().parent.parent
        script=sdk/"lib/gcloud.py"
        if not script.is_file():raise RuntimeError("Install the official Google Cloud SDK with its Python entry point.")
        bundled=sdk/"platform/bundledpython/python.exe"
        interpreter=str(bundled) if bundled.is_file() else sys.executable
        return [interpreter,str(script),*arguments[1:]]
    return [executable,*arguments[1:]]
