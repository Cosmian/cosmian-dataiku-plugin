from cosmian_lib import Context
from .computations import Computations
from .authentication import Authentication


class Orchestrator():
    def __init__(self, orchestrator_server_url):
        if orchestrator_server_url.endswith("/"):
            url = orchestrator_server_url[:len(orchestrator_server_url) - 1]
        else:
            url = orchestrator_server_url
        self.context = Context(url)

    def authentication(self) -> Authentication:
        """
        Access the Authentication API
        """
        return Authentication(self.context)

    def computations(self) -> Computations:
        """
        Access the Computations management API
        """
        return Computations(self.context)
