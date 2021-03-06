from cosmian_lib import Context


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
        # TODO hash password see https://cosmian.atlassian.net/browse/CCO-127
        return self.context.post("/auth/login", {"email": username, "password": password}, "Login:: authentication failed")

    def logout(self):
        """
        Logout from  the orchestrator
        """
        # TODO hash password see https://cosmian.atlassian.net/browse/CCO-127
        return self.context.post("/auth/logout", {}, "Logout:: failed")
