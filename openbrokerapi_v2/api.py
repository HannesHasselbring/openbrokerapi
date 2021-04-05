import logging
from dataclasses import dataclass
from typing import List, Union

from fastapi import APIRouter, Header, status
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Router

from openbrokerapi_v2 import errors
from openbrokerapi_v2.helper import ensure_list
from openbrokerapi_v2.response import (
    CatalogResponse,
    ProvisioningResponse,
    ErrorResponse,
    UpdateResponse,
)
from openbrokerapi_v2.router import Router
from openbrokerapi_v2.service_broker import (
    ServiceBroker,
    ProvisionDetails,
    ProvisionState,
    UpdateDetails,
)


@dataclass
class BrokerCredentials:
    """
    Credentials, which will be used to validate authenticate requests
    """

    username: str
    password: str


def _check_plan_id(broker: ServiceBroker, plan_id) -> bool:
    """
    Checks that the plan_id exists in the catalog
    :return: boolean
    """
    for service in ensure_list(broker.catalog()):
        for plan in service.plans:
            if plan.id == plan_id:
                return True
    return False


def get_router(
    service_broker: ServiceBroker,
    broker_credentials: Union[None, List[BrokerCredentials], BrokerCredentials],
    logger: logging.Logger,
) -> APIRouter:
    """
    Returns the blueprint with service broker api.

    :param service_broker: Services that this broker exposes
    :param broker_credentials: Optional Usernames and passwords that will be required to communicate with service broker
    :param logger: Used for api logs. This will not influence Flasks logging behavior.
    :return: Blueprint to register with Flask app instance
    """
    openbroker = APIRouter()

    # # Apply filters
    # logger.debug("Apply print_request filter for debugging")
    # openbroker.before_request(print_request)
    # # TODO how to do before request?
    #
    # if DISABLE_VERSION_CHECK:
    #     logger.warning(
    #         "Minimum API version is not checked, this can cause illegal contracts between service broker and platform!"
    #     )
    # else:
    #     logger.debug("Apply check_version filter for version %s" % str(MIN_VERSION))
    #     openbroker.before_request(check_version)
    #
    # logger.debug("Apply check_originating_identity filter")
    # openbroker.before_request(check_originating_identity)
    #
    # if broker_credentials is not None:
    #     broker_credentials = ensure_list(broker_credentials)
    #     logger.debug("Apply check_auth filter with {} credentials".format(len(broker_credentials)))
    #     openbroker.before_request(get_auth_filter(broker_credentials))
    #
    # # TODO how to extract username?
    # def extract_authorization_username(request: Request):
    #     if request.authorization is not None:
    #         return request.authorization.username
    #     else:
    #         return None
    #
    # # TODO how to do errorhandling
    # @openbroker.exceotuin(Exception)
    # def error_handler(e):
    #     logger.exception(e)
    #     return to_json_response(ç(
    #         description=constants.DEFAULT_EXCEPTION_ERROR_MESSAGE
    #     )), HTTPStatus.INTERNAL_SERVER_ERROR
    #
    # @openbroker.errorhandler(NotImplementedError)
    # def error_handler_not_implemented(e):
    #     logger.exception(e)
    #     return to_json_response(ErrorResponse(
    #         description=constants.DEFAULT_NOT_IMPLEMENTED_ERROR_MESSAGE
    #     )), HTTPStatus.NOT_IMPLEMENTED
    #
    # @openbroker.errorhandler(errors.ErrBadRequest)
    # def error_handler_bad_request(e):
    #     logger.exception(e)
    #     return to_json_response(ErrorResponse(
    #         description=constants.DEFAULT_BAD_REQUEST_ERROR_MESSAGE
    #     )), HTTPStatus.BAD_REQUEST

    @openbroker.get(
        "/v2/catalog", response_model_exclude_none=True, response_model=CatalogResponse
    )
    def catalog() -> CatalogResponse:
        """
        :return: Catalog of broker (List of services)
        """
        catalog = ensure_list(service_broker.catalog())
        return CatalogResponse(services=catalog)

    @openbroker.put(
        "/v2/service_instances/{instance_id}",
        response_model=ProvisioningResponse,
        responses={
            status.HTTP_200_OK: {"model": ProvisioningResponse},
            status.HTTP_201_CREATED: {"model": ProvisioningResponse},
            status.HTTP_202_ACCEPTED: {"model": ProvisioningResponse},
            status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse},
            status.HTTP_409_CONFLICT: {"model": {}},
            status.HTTP_422_UNPROCESSABLE_ENTITY: {"model": ErrorResponse},
        },
    )
    def provision(
        request: Request,
        instance_id,
        provision_details: ProvisionDetails,
        accepts_incomplete: bool = False,
        x_broker_api_version: str = Header(None),
    ) -> JSONResponse:
        """
        Provision....
        """
        # TODO: Authorization
        logger.debug(f"X-Broker-Api-Version: {x_broker_api_version}")
        if request.headers.get("content-type") != "application/json":
            return JSONResponse(
                content=ErrorResponse(
                    error="ContentTypeError",
                    description='Improper Content-Type header. Expecting "application/json"',
                ).json(),
                status_code=status.HTTP_400_BAD_REQUEST,
            )

        if not _check_plan_id(service_broker, provision_details.plan_id):
            raise TypeError("plan_id not found in this service.")
        try:
            result = service_broker.provision(
                instance_id=instance_id, details=provision_details, async_allowed=accepts_incomplete
            )
        except errors.ErrInstanceAlreadyExists as e:
            logger.exception(e)
            return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={})
        except errors.ErrAsyncRequired as e:
            logger.exception(e)
            return JSONResponse(
                content=ErrorResponse(
                    error="AsyncRequired",
                    description="This service plan requires client support for asynchronous service operations.",
                ).json(),
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )
        if result.state == ProvisionState.IS_ASYNC:
            return JSONResponse(
                content=ProvisioningResponse(
                    dashboard_url=result.dashboard_url, operation=result.operation
                ).json(),
                status_code=status.HTTP_202_ACCEPTED,
            )
        elif result.state == ProvisionState.IDENTICAL_ALREADY_EXISTS:
            return JSONResponse(
                content=ProvisioningResponse(
                    dashboard_url=result.dashboard_url, operation=result.operation
                ).json(),
                status_code=status.HTTP_200_OK,
            )
        elif result.state == ProvisionState.SUCCESSFUL_CREATED:
            return JSONResponse(
                content=ProvisioningResponse(
                    dashboard_url=result.dashboard_url, operation=result.operation
                ).json(),
                status_code=status.HTTP_201_CREATED,
            )
        else:
            raise errors.ServiceException("IllegalState, ProvisioningState unknown.")

    @openbroker.patch(
        "/v2/service_instances/{instance_id}",
        response_model=UpdateResponse,
        responses={
            status.HTTP_200_OK: {"model": ProvisioningResponse},
            status.HTTP_202_ACCEPTED: {"model": ProvisioningResponse},
            status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse},
            status.HTTP_409_CONFLICT: {"model": {}},
            status.HTTP_422_UNPROCESSABLE_ENTITY: {"model": ErrorResponse},
        },
    )
    def update(
        instance_id,
        update_details: UpdateDetails,
        accepts_incomplete: bool = False,
        x_broker_api_version: str = Header(None),
    ):
        try:
            # TODO: Authentication, originating_identity
            plan_id = update_details.plan_id
            if plan_id and not _check_plan_id(service_broker, plan_id):
                raise TypeError("plan_id not found in this service.")

            result = service_broker.update(
                instance_id=instance_id, details=update_details, async_allowed=accepts_incomplete
            )
        except errors.ErrInstanceAlreadyExists as e:
            logger.exception(e)
            return JSONResponse(status_code=status.HTTP_409_CONFLICT, content={})

        except errors.ErrAsyncRequired as e:
            logger.exception(e)
            return JSONResponse(
                ErrorResponse(
                    error="AsyncRequired",
                    description="This service plan requires client support for asynchronous service operations.",
                ).json(),
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            )

        return JSONResponse(
            UpdateResponse(operation=result.operation, dashboard_url=result.dashboard_url).json(), status_code=status.HTTP_200_OK
        )

    #
    # @openbroker.route("/v2/service_instances/<instance_id>/service_bindings/<binding_id>", methods=['PUT'])
    # @requires_application_json
    # def bind(instance_id, binding_id):
    #     try:
    #         accepts_incomplete = 'true' == request.args.get("accepts_incomplete", 'false')
    #
    #         binding_details = BindDetails(**json.loads(request.data))
    #         binding_details.originating_identity = request.originating_identity
    #         binding_details.authorization_username = extract_authorization_username(request)
    #         if not _check_plan_id(service_broker, binding_details.plan_id):
    #             raise TypeError('plan_id not found in this service.')
    #     except (TypeError, KeyError, JSONDecodeError) as e:
    #         logger.exception(e)
    #         return to_json_response(ErrorResponse(description=str(e))), HTTPStatus.BAD_REQUEST
    #
    #     try:
    #         result = service_broker.bind(instance_id, binding_id, binding_details, accepts_incomplete)
    #     except errors.ErrBindingAlreadyExists as e:
    #         logger.exception(e)
    #         return to_json_response(EmptyResponse()), HTTPStatus.CONFLICT
    #     except errors.ErrAppGuidNotProvided as e:
    #         logger.exception(e)
    #         return to_json_response(ErrorResponse(
    #             error="RequiresApp",
    #             description="This service supports generation of credentials through binding an application only."
    #         )), HTTPStatus.UNPROCESSABLE_ENTITY
    #
    #     response = BindResponse(
    #         credentials=result.credentials,
    #         syslog_drain_url=result.syslog_drain_url,
    #         route_service_url=result.route_service_url,
    #         volume_mounts=result.volume_mounts
    #     )
    #     if result.state == BindState.SUCCESSFUL_BOUND:
    #         return to_json_response(response), HTTPStatus.CREATED
    #     elif result.state == BindState.IDENTICAL_ALREADY_EXISTS:
    #         return to_json_response(response), HTTPStatus.OK
    #     elif result.state == BindState.IS_ASYNC:
    #         return to_json_response(BindResponse(operation=result.operation)), HTTPStatus.ACCEPTED
    #     else:
    #         raise errors.ServiceException('IllegalState, BindState unknown.')
    #
    # @openbroker.route("/v2/service_instances/<instance_id>/service_bindings/<binding_id>", methods=['DELETE'])
    # def unbind(instance_id, binding_id):
    #     try:
    #         accepts_incomplete = 'true' == request.args.get("accepts_incomplete", 'false')
    #
    #         plan_id = request.args["plan_id"]
    #         service_id = request.args["service_id"]
    #
    #         unbind_details = UnbindDetails(service_id=service_id, plan_id=plan_id)
    #         unbind_details.originating_identity = request.originating_identity
    #         unbind_details.authorization_username = extract_authorization_username(request)
    #         if not _check_plan_id(service_broker, unbind_details.plan_id):
    #             raise TypeError('plan_id not found in this service.')
    #     except (TypeError, KeyError) as e:
    #         logger.exception(e)
    #         return to_json_response(ErrorResponse(description=str(e))), HTTPStatus.BAD_REQUEST
    #
    #     try:
    #         result = service_broker.unbind(instance_id, binding_id, unbind_details, accepts_incomplete)
    #     except errors.ErrBindingDoesNotExist as e:
    #         logger.exception(e)
    #         return to_json_response(EmptyResponse()), HTTPStatus.GONE
    #
    #     if result.is_async:
    #         return to_json_response(UnbindResponse(result.operation)), HTTPStatus.ACCEPTED
    #     else:
    #         return to_json_response(EmptyResponse()), HTTPStatus.OK
    #
    # @openbroker.route("/v2/service_instances/<instance_id>", methods=['DELETE'])
    # def deprovision(instance_id):
    #     try:
    #         plan_id = request.args["plan_id"]
    #         service_id = request.args["service_id"]
    #         accepts_incomplete = 'true' == request.args.get("accepts_incomplete", 'false')
    #
    #         deprovision_details = DeprovisionDetails(service_id=service_id, plan_id=plan_id)
    #         deprovision_details.originating_identity = request.originating_identity
    #         deprovision_details.authorization_username = extract_authorization_username(request)
    #         if not _check_plan_id(service_broker, deprovision_details.plan_id):
    #             raise TypeError('plan_id not found in this service.')
    #     except (TypeError, KeyError) as e:
    #         logger.exception(e)
    #         return to_json_response(ErrorResponse(description=str(e))), HTTPStatus.BAD_REQUEST
    #
    #     try:
    #         result = service_broker.deprovision(instance_id, deprovision_details, accepts_incomplete)
    #     except errors.ErrInstanceDoesNotExist as e:
    #         logger.exception(e)
    #         return to_json_response(EmptyResponse()), HTTPStatus.GONE
    #     except errors.ErrAsyncRequired as e:
    #         logger.exception(e)
    #         return to_json_response(ErrorResponse(
    #             error="AsyncRequired",
    #             description="This service plan requires client support for asynchronous service operations."
    #         )), HTTPStatus.UNPROCESSABLE_ENTITY
    #
    #     if result.is_async:
    #         return to_json_response(DeprovisionResponse(result.operation)), HTTPStatus.ACCEPTED
    #     else:
    #         return to_json_response(EmptyResponse()), HTTPStatus.OK
    #
    # @openbroker.route("/v2/service_instances/<instance_id>/last_operation", methods=['GET'])
    # def last_operation(instance_id):
    #     # TODO: forward them
    #     # service_id = request.args.get("service_id", None)
    #     # plan_id = request.args.get("plan_id", None)
    #
    #     operation_data = request.args.get("operation", None)
    #
    #     try:
    #         result = service_broker.last_operation(instance_id, operation_data)
    #         return to_json_response(LastOperationResponse(result.state, result.description)), HTTPStatus.OK
    #     except errors.ErrInstanceDoesNotExist:
    #         return to_json_response(LastOperationResponse(OperationState.SUCCEEDED, '')), HTTPStatus.GONE
    #
    # @openbroker.route("/v2/service_instances/<instance_id>/service_bindings/<binding_id>/last_operation",
    #                   methods=['GET'])
    # def last_binding_operation(instance_id, binding_id):
    #     # TODO: forward them
    #     # service_id = request.args.get("service_id", None)
    #     # plan_id = request.args.get("plan_id", None)
    #
    #     operation_data = request.args.get("operation", None)
    #     result = service_broker.last_binding_operation(instance_id, binding_id, operation_data)
    #     return to_json_response(LastOperationResponse(result.state, result.description)), HTTPStatus.OK
    #
    # @openbroker.route("/v2/service_instances/<instance_id>", methods=['GET'])
    # def get_instance(instance_id):
    #     try:
    #         result = service_broker.get_instance(instance_id)
    #         response = GetInstanceResponse(
    #             service_id=result.service_id,
    #             plan_id=result.plan_id,
    #             dashboard_url=result.dashboard_url,
    #             parameters=result.parameters,
    #         )
    #         return to_json_response(response), HTTPStatus.OK
    #
    #     except errors.ErrInstanceDoesNotExist:
    #         return to_json_response(EmptyResponse()), HTTPStatus.NOT_FOUND
    #     except errors.ErrConcurrentInstanceAccess:
    #         error_response = ErrorResponse(error='ConcurrencyError',
    #                                        description='The Service Broker does not support concurrent requests that mutate the same resource.')
    #         return to_json_response(error_response), HTTPStatus.UNPROCESSABLE_ENTITY
    #
    # @openbroker.route("/v2/service_instances/<instance_id>/service_bindings/<binding_id>", methods=['GET'])
    # def get_binding(instance_id, binding_id):
    #     try:
    #         result = service_broker.get_binding(instance_id, binding_id)
    #         response = GetBindingResponse(
    #             credentials=result.credentials,
    #             syslog_drain_url=result.syslog_drain_url,
    #             route_service_url=result.route_service_url,
    #             volume_mounts=result.volume_mounts,
    #             parameters=result.parameters,
    #         )
    #         return to_json_response(response), HTTPStatus.OK
    #     except errors.ErrBindingDoesNotExist:
    #         return to_json_response(EmptyResponse()), HTTPStatus.NOT_FOUND

    return openbroker


def serve_multiple(
    service_brokers: List[ServiceBroker],
    credentials: Union[List[BrokerCredentials], BrokerCredentials, None],
    logger: logging.Logger = logging.root,
    port=5000,
    debug=False,
):
    router = Router(*service_brokers)
    serve(router, credentials, logger, port, debug)


def serve(
    service_broker: ServiceBroker,
    credentials: Union[List[BrokerCredentials], BrokerCredentials, None],
    logger: logging.Logger = logging.root,
    port=5000,
    debug=False,
):
    """
    Starts fastAPI with the given brokers.
    You can provide a list or just one ServiceBroker

    :param service_broker: ServicesBroker for services to provide
    :param credentials: Username and password that will be required to communicate with service broker
    :param logger: Used for api logs. This will not influence Flasks logging behavior
    :param port: Port
    :param debug: Enables debugging in flask app
    """
    from fastapi import FastAPI

    app = FastAPI()
    app.debug = debug

    router = get_router(service_broker, credentials, logger)

    logger.debug("Register openbrokerapi_v2 blueprint")
    app.include_router(router=router)

    logger.info("Start Gevent server on 0.0.0.0:%s" % port)
    import uvicorn

    uvicorn.run(
        app, host="127.0.0.1", port=port, log_level="debug",
    )
