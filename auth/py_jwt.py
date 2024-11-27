import time
import jwt
from fastapi import Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from auth import py_keys


def signJWT(username, user_fk, comp_fk, role_fk, comp_name):
    payload = {
        "username": username,
        "user_fk": user_fk,
        "comp_fk": comp_fk,
        "role_fk": role_fk,
        "comp_name": comp_name,
        "expiry": time.time() + py_keys.exp_hrs * 3600,  # number of hours
    }

    token = jwt.encode(payload, py_keys.jwt_secret, algorithm=py_keys.jwt_algorithm)
    return token


class JWTBearer(HTTPBearer):
    def __init__(self, auto_error: bool = True):
        super(JWTBearer, self).__init__(auto_error=auto_error)
        self.payload = {}

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(JWTBearer, self).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(status_code=401, detail="Invalid authentication scheme.")
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(status_code=401, detail="Invalid token or expired token.")
            return self.payload
        else:
            raise HTTPException(status_code=401, detail="Invalid authorization code.")

    def verify_jwt(self, jwtoken: str) -> bool:
        isTokenValid: bool = False
        try:
            self.payload = self.decodeJWT(jwtoken)
        except:
            self.payload = None
        if self.payload:
            isTokenValid = True
        return isTokenValid

    def decodeJWT(self, token: str):
        try:
            if token:
                decoded = jwt.decode(token, py_keys.jwt_secret, algorithms=py_keys.jwt_algorithm)
                return decoded if decoded['expiry'] >= time.time() else None
            else:
                return None
        except Exception as e:
            print("decodeJWT " + str(e))
            return None