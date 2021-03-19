from typing import List, Optional

from pydantic import BaseModel, Extra

from openbrokerapi_v2.service_broker import OperationState, VolumeMount, Service


class ErrorResponse(BaseModel):
    error: Optional[str]
    description: str


class CatalogResponse(BaseModel):
    services: List[Service]

    class Config:
        extra = Extra.allow


class ProvisioningResponse(BaseModel):
    dashboard_url: str
    operation: str


class AsyncResponse:
    pass


class GetInstanceResponse:
    def __init__(
        self,
        service_id: str,
        plan_id: str,
        dashboard_url: Optional[str] = None,
        parameters: Optional[dict] = None,
    ):
        self.service_id = service_id
        self.plan_id = plan_id
        self.dashboard_url = dashboard_url
        self.parameters = parameters


class BindResponse:
    def __init__(
        self,
        credentials: dict = None,
        syslog_drain_url: str = None,
        route_service_url: str = None,
        volume_mounts: List[VolumeMount] = None,
        operation: Optional[str] = None,
    ):
        self.credentials = credentials
        self.syslog_drain_url = syslog_drain_url
        self.route_service_url = route_service_url
        self.volume_mounts = volume_mounts
        self.operation = operation


class GetBindingResponse:
    def __init__(
        self,
        credentials: dict = None,
        syslog_drain_url: str = None,
        route_service_url: str = None,
        volume_mounts: List[VolumeMount] = None,
        parameters: Optional[dict] = None,
    ):
        self.credentials = credentials
        self.syslog_drain_url = syslog_drain_url
        self.route_service_url = route_service_url
        self.volume_mounts = volume_mounts
        self.parameters = parameters


class UnbindResponse(AsyncResponse):
    def __init__(self, operation: str):
        self.operation = operation


class UpdateResponse(AsyncResponse):
    def __init__(self, operation: Optional[str], dashboard_url: Optional[str]):
        self.operation = operation
        self.dashboard_url = dashboard_url


class DeprovisionResponse(AsyncResponse):
    def __init__(self, operation: str):
        self.operation = operation


class LastOperationResponse:
    def __init__(self, state: OperationState, description: str):
        self.state = state.value
        self.description = description
