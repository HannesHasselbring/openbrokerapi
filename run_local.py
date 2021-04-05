from unittest.mock import Mock

from openbrokerapi_v2.api import serve, BrokerCredentials

if __name__ == "__main__":
    broker = Mock()
    broker.catalog.return_value = []
    serve(broker, BrokerCredentials("", ""))