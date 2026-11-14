"""Authenticated operator demonstration over HTTPS; never accepts cloud mode anonymously."""
import hmac
import os
from urllib.parse import urlparse
from flask import Response,request
from werkzeug.security import check_password_hash

class CloudAccess:
    def __init__(self,environ=None):
        env=os.environ if environ is None else environ
        self.origin=env.get("WEB_PUBLIC_ORIGIN","")
        parsed=urlparse(self.origin)
        self.user=env.get("WEB_OPERATOR_USER","")
        self.password_hash=env.get("WEB_OPERATOR_PASSWORD_HASH","")
        if parsed.scheme!="https" or not parsed.hostname or parsed.path or parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise RuntimeError("The cloud web interface requires its canonical HTTPS origin.")
        if not self.user or not self.password_hash.startswith(("scrypt:","pbkdf2:sha256:")):
            raise RuntimeError("The cloud web interface requires a server-side operator password hash.")
        self.host=parsed.netloc.lower()
    def authorize(self):
        if request.path=="/health":return None
        if request.host.lower()!=self.host:return Response("Unknown web host.",status=400)
        if not request.is_secure:return Response("HTTPS is required.",status=403)
        auth=request.authorization
        valid=bool(auth and auth.type=="basic" and hmac.compare_digest(auth.username or "",self.user))
        try:valid=valid and check_password_hash(self.password_hash,auth.password or "")
        except (ValueError,TypeError):valid=False
        if not valid:return Response("Authentication required.",401,{"WWW-Authenticate":'Basic realm="Supper operator demo", charset="UTF-8"',"Cache-Control":"no-store"})
        return None
