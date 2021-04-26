from ..context import Context
import hashlib


class Authentication():

    def __init__(self, context: Context):
        self.context = context

    def login(self, username: str, password: str) -> dict:
        """
        Login to the orchestrator

        Returns an User dict
        User {
            uuid: str,
            first_name: str,
            last_name: str,
            avatar: str,  #if exists
            email: str,
            created_at: str, #ISO format
            computations: [str],
            approvals: [str],
            permissions: [str],
            is_admin: bool,
        }
        """
        hashed_pass = hashlib.sha256(password.encode())
        return self.context.post("/auth/login", {"email": username, "password": hashed_pass}, "Login:: authentication failed")

    def logout(self):
        """
        Logout from  the orchestrator
        """
        return self.context.post("/auth/logout", {}, "Logout:: failed")
