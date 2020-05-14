from .context import Context
from .views import Views
from .psi import PSI
from .datasets import Datasets
from .enclave import Enclave
from .fe import FE
from .mpc import MPC


class Server():
    def __init__(self, cosmian_server_url):
        # server can also be instantiated from an existing context
        # but we do not want to advertise that Server.from_context() should be used
        if isinstance(cosmian_server_url, Context):
            self.context = cosmian_server_url
            return
        if cosmian_server_url.endswith("/"):
            url = cosmian_server_url[:len(cosmian_server_url) - 1]
        else:
            url = cosmian_server_url
        self.context = Context(url)

    def views(self) -> Views:
        """
        Access to views management.
        A view defines how data in an underlying data source is laid out and accessed.
        A view can also define transformations to be applied on the fly to the raw data when accessed.
        """
        return Views(self.context)

    def datasets(self) -> Datasets:
        """
        Access to the datasets management
        Datasets are data sources exposed through a 'view'.
        There data is laid out according to a 'schema' which is part of the 'view'
        """
        return Datasets(self.context)

    def psi(self) -> PSI:
        """
        Access to Private Set Intersection primitives.
        PSI allows two parties holding sets to compare encrypted versions
        of these sets in order to compute the intersection.
        Neither party reveals anything to the other party except for the intersection
        which is revealed to one of the parties.
        """
        return PSI(self.context)

    def enclave(self) -> Enclave:
        """
        Access to the enclaves management routines
        These routines only work if the server is an SGX machine
        """
        return Enclave(self.context)

    def fe(self) -> FE:
        """
        Access to the functional encryption primitives
        """
        return FE(self.context)

    def mpc(self) -> MPC:
        """
        Access to the multi-party computation primitives
        """
        return MPC(self.context)

    @classmethod
    def from_context(cls, context: Context):
        """
        Instantiate a server instance from an existing context
        """
        return cls(context)
