import http
import json

import openbrokerapi_v2
from openbrokerapi_v2 import errors

from openbrokerapi_v2.service_broker import (
    ProvisionedServiceSpec,
    ProvisionDetails,
    ProvisionState,
)


# class ProvisioningTest(BrokerTestCase):
#     def setUp(self):
#         self.broker.catalog.return_value = [
#             Service(
#                 id='service-guid-here',
#                 name='',
#                 description='',
#                 bindable=True,
#                 plans=[
#                     ServicePlan('plan-guid-here', name='', description='')
#                 ])
#         ]
from tests import AUTH_HEADER


def test_provisioning_called_with_the_right_values(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        dashboard_url="dash_url", operation="operation_str"
    )

    response = client.put(
        "/v2/service_instances/here-instance-id?accepts_incomplete=true",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
                "parameters": {"parameter1": 1},
                "context": {
                    "organization_guid": "org-guid-here",
                    "space_guid": "space-guid-here",
                },
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    (
        actual_instance_id,
        actual_details,
        actual_async_allowed,
    ) = mock_broker.provision.call_args[0]
    assert actual_instance_id == "here-instance-id"
    assert actual_async_allowed

    assert type(actual_details) is ProvisionDetails
    assert actual_details.service_id == "s1"
    assert actual_details.plan_id == "p1"
    assert actual_details.parameters == dict(parameter1=1)
    assert actual_details.organization_guid == "org-guid-here"
    assert actual_details.space_guid == "space-guid-here"
    assert actual_details.context["organization_guid"] == "org-guid-here"
    assert actual_details.context["space_guid"] == "space-guid-here"
    assert response.json() is not None


def test_provisining_called_just_with_required_fields(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        dashboard_url="dash_url", operation="operation_str"
    )

    response = client.put(
        "/v2/service_instances/here-instance-id",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "context": {
                    "organization_guid": "org-guid-here",
                    "space_guid": "space-guid-here",
                },
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    (
        actual_instance_id,
        actual_details,
        actual_async_allowed,
    ) = mock_broker.provision.call_args[0]
    assert actual_instance_id == "here-instance-id"
    assert not actual_async_allowed
    assert type(actual_details) is ProvisionDetails
    assert actual_details.service_id == "s1"
    assert actual_details.plan_id == "p1"
    assert actual_details.context["organization_guid"] == "org-guid-here"
    assert actual_details.context["space_guid"] == "space-guid-here"

    assert actual_details.parameters is None
    assert response.json() is not None


def test_provisining_optional_org_and_space_if_available_in_context(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        dashboard_url="dash_url", operation="operation_str"
    )

    client.put(
        "/v2/service_instances/here-instance-id",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    (
        actual_instance_id,
        actual_details,
        actual_async_allowed,
    ) = mock_broker.provision.call_args[0]
    assert actual_instance_id == "here-instance-id"
    assert not actual_async_allowed

    assert type(actual_details) == ProvisionDetails
    assert actual_details.service_id == "s1"
    assert actual_details.plan_id == "p1"
    assert actual_details.organization_guid == "org-guid-here"
    assert actual_details.space_guid == "space-guid-here"

    assert actual_details.parameters is None


def test_provisining_ignores_unknown_parameters(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        dashboard_url="dash_url", operation="operation_str"
    )

    client.put(
        "/v2/service_instances/here-instance-id",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
                "unknown": "unknown",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    (
        actual_instance_id,
        actual_details,
        actual_async_allowed,
    ) = mock_broker.provision.call_args[0]
    assert actual_instance_id == "here-instance-id"
    assert not actual_async_allowed

    assert type(actual_details) == ProvisionDetails
    assert actual_details.service_id == "s1"
    assert actual_details.plan_id == "p1"
    assert actual_details.organization_guid == "org-guid-here"
    assert actual_details.space_guid == "space-guid-here"

    assert actual_details.parameters is None


def test_returns_201_if_created(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        dashboard_url="dash_url", operation="operation_str"
    )

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code, http.HTTPStatus.CREATED
    assert response.json() == json.dumps(
        dict(dashboard_url="dash_url", operation="operation_str")
    )


def test_returns_202_if_provisioning_in_progress(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        state=ProvisionState.IS_ASYNC,
        dashboard_url="dash_url",
        operation="operation_str",
    )

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code, http.HTTPStatus.ACCEPTED
    assert response.json() == json.dumps(
        dict(dashboard_url="dash_url", operation="operation_str")
    )


def test_returns_409_if_already_exists_but_is_not_equal(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.side_effect = errors.ErrInstanceAlreadyExists()

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code, http.HTTPStatus.CONFLICT
    assert response.json() == {}


def test_returns_422_if_async_required_but_not_supported(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.side_effect = errors.ErrAsyncRequired()

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code, http.HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == json.dumps(
        dict(
            error="AsyncRequired",
            description="This service plan requires client support for asynchronous service operations.",
        )
    )


def test_returns_400_if_missing_mandatory_data(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    # mock_broker.provision.side_effect = errors.ErrInvalidParameters(
    #     "Required parameters not provided."
    # )

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                # "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.BAD_REQUEST
    assert "value_error.missing" in response.json()["description"]


def test_returns_400_if_missing_org_and_space_guids_data(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = (
        mock_broker.provision.return_value
    ) = ProvisionedServiceSpec(
        state=ProvisionState.IS_ASYNC,
        dashboard_url="dash_url",
        operation="operation_str",
    )

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps({"service_id": "s1", "p1": "plan-guid-here"}),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert http.HTTPStatus.BAD_REQUEST == response.status_code
    assert "value_error.missing" in response.json()["description"]


def test_returns_202_if_missing_org_and_space_guids_data_org_space_check_flag_true(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    openbrokerapi_v2.service_broker.DISABLE_SPACE_ORG_GUID_CHECK = True

    mock_broker.provision.return_value = (
        mock_broker.provision.return_value
    ) = ProvisionedServiceSpec(
        state=ProvisionState.IS_ASYNC,
        dashboard_url="dash_url",
        operation="operation_str",
    )
    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps({"service_id": "s1", "plan_id": "p1",}),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.ACCEPTED
    assert response.json() == json.dumps(
        dict(dashboard_url="dash_url", operation="operation_str")
    )

    openbrokerapi_v2.service_broker.DISABLE_SPACE_ORG_GUID_CHECK = False


def test_returns_200_if_identical_service_exists(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    mock_broker.provision.return_value = ProvisionedServiceSpec(
        state=ProvisionState.IDENTICAL_ALREADY_EXISTS,
        dashboard_url="dash_url",
        operation="operation_str",
    )

    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.OK
    assert response.json() == dict()


def test_returns_400_if_request_does_not_contain_content_type_header(
    client, mock_broker, demo_service
):
    mock_broker.catalog.return_value = demo_service
    response = client.put(
        "/v2/service_instances/abc",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
            }
        ),
        headers={"X-Broker-Api-Version": "2.13", "Authorization": AUTH_HEADER,},
    )

    assert response.status_code == http.HTTPStatus.BAD_REQUEST
    assert 'Expecting "application/json"' in json.loads(response.json()).get(
        "description"
    )


def test_returns_400_if_request_does_not_contain_valid_json_body(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    response = client.put(
        "/v2/service_instances/abc",
        data="I am not a json object",
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.BAD_REQUEST
    assert "error.jsondecode" in response.json().get("description")


def test_returns_400_if_context_organization_guid_mismatch(
    mock_broker, client, demo_service
):
    mock_broker.catalog.return_value = demo_service
    response = client.put(
        "/v2/service_instances/here-instance-id?accepts_incomplete=true",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
                "parameters": {"parameter1": 1},
                "context": {
                    "organization_guid": "a_mismatching_org",
                    "space_guid": "space-guid-here",
                },
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.BAD_REQUEST
    assert "foo" in json.loads(response.json()).get("description")


def test_returns_400_if_context_space_guid_mismatch(mock_broker, client, demo_service):
    mock_broker.catalog.return_value = demo_service
    response = client.put(
        "/v2/service_instances/here-instance-id?accepts_incomplete=true",
        data=json.dumps(
            {
                "service_id": "s1",
                "plan_id": "p1",
                "organization_guid": "org-guid-here",
                "space_guid": "space-guid-here",
                "parameters": {"parameter1": 1},
                "context": {
                    "organization_guid": "org-guid-here",
                    "space_guid": "a_mismatching_space",
                },
            }
        ),
        headers={
            "X-Broker-Api-Version": "2.13",
            "Content-Type": "application/json",
            "Authorization": AUTH_HEADER,
        },
    )

    assert response.status_code == http.HTTPStatus.BAD_REQUEST
    assert "foo" in json.loads(response.json()).get("description")
