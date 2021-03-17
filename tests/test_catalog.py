import base64
import http


from openbrokerapi_v2 import constants

from openbrokerapi_v2.catalog import (
    ServiceDashboardClient,
    ServiceMetadata,
    ServicePlan,
    ServicePlanCost,
    ServicePlanMetadata,
)
from openbrokerapi_v2.log_util import configure
from openbrokerapi_v2.service_broker import Service, ServiceBroker

def test_catalog_called_with_the_right_values(client, demo_service, mock_broker):
    mock_broker.catalog.return_value = demo_service

    client.get(
        "/v2/catalog",
        headers={"X-Broker-Api-Version": "2.13", "Authorization": AUTH_HEADER},
    )

    assert mock_broker.catalog.called


def test_catalog_ignores_request_headers(client, demo_service, mock_broker):
    mock_broker.catalog.return_value = demo_service

    client.get(
        "/v2/catalog",
        headers={
            "X-Broker-Api-Version": "2.13",
            "Authorization": AUTH_HEADER,
            "unknown": "unknown",
        },
    )

    assert mock_broker.catalog.called


def test_catalog_returns_200_with_service_information(
        client, demo_service, mock_broker
):
    """
    This tests all the possible information a catalog could have. Including custom Service-/ServicePlanMetadata
    """
    mock_broker.catalog.return_value = Service(
        id="s1",
        name="service_name",
        description="service_description",
        bindable=True,
        plans=[
            ServicePlan(
                id="p1",
                name="default",
                description="plan_description",
                metadata=ServicePlanMetadata(
                    displayName="displayName",
                    bullets=["bullet1"],
                    costs=[ServicePlanCost(amount={"requests": 1}, unit="unit")],
                    custom_field2="custom_field2",
                ),
                free=True,
                bindable=True,
            )
        ],
        tags=["tag1", "tag2"],
        requires=["something"],
        metadata=ServiceMetadata(
            displayName="displayName",
            imageUrl="imageUrl",
            longDescription="longDescription",
            providerDisplayName="providerDisplayName",
            documentationUrl="documentationUrl",
            supportUrl="supportUrl",
            shareable=True,
            custom_field1="custom_field1",
        ),
        dashboard_client=ServiceDashboardClient(
            id="id", secret="secret", redirect_uri="redirect_uri"
        ),
        plan_updateable=True,
    )

    response = client.get(
        "/v2/catalog",
        headers={
            "X-Broker-Api-Version": "2.13",
            "Authorization": AUTH_HEADER,
            "unknown": "unknown",
        },
    )

    assert response.status_code == http.HTTPStatus.OK
    assert response.json() == dict(
        services=[
            dict(
                id="s1",
                name="service_name",
                description="service_description",
                instances_retrievable=False,
                bindings_retrievable=False,
                bindable=True,
                plans=[
                    dict(
                        id="p1",
                        name="default",
                        description="plan_description",
                        metadata=dict(
                            displayName="displayName",
                            bullets=["bullet1"],
                            costs=[dict(amount={"requests": 1.0}, unit="unit")],
                            custom_field2="custom_field2",
                        ),
                        free=True,
                        bindable=True,
                    )
                ],
                tags=["tag1", "tag2"],
                requires=["something"],
                metadata=dict(
                    displayName="displayName",
                    imageUrl="imageUrl",
                    longDescription="longDescription",
                    providerDisplayName="providerDisplayName",
                    documentationUrl="documentationUrl",
                    supportUrl="supportUrl",
                    shareable=True,
                    custom_field1="custom_field1",
                ),
                dashboard_client=dict(
                    id="id", secret="secret", redirect_uri="redirect_uri"
                ),
                plan_updateable=True,
            )
        ]
    )


def test_catalog_returns_200_with_minimal_service_information(
        client, demo_service, mock_broker
):
    mock_broker.catalog.return_value = demo_service

    response = client.get(
        "/v2/catalog",
        headers={
            "X-Broker-Api-Version": "2.13",
            "X-Broker-Api-Originating-Identity": "test "
                                                 + base64.standard_b64encode(b'{"user_id":123}').decode("ascii"),
            "Authorization": AUTH_HEADER,
            "unknown": "unknown",
        },
    )

    assert http.HTTPStatus.OK == response.status_code
    assert response.json() == dict(
        services=[
            dict(
                id="s1",
                name="service_name",
                description="service_description",
                instances_retrievable=False,
                bindings_retrievable=False,
                bindable=True,
                plan_updateable=False,
                plans=[
                    dict(id="p1", name="default", description="plan_description")
                ],
            )
        ])


def test_catalog_returns_500_if_error_raised(client, demo_service, mock_broker):
    # TODO is this a problem only in this test?
    mock_broker.catalog.side_effect = Exception("ERROR")
    mock_broker.catalog.return_value = demo_service

    response = client.get(
        "/v2/catalog",
        headers={
            "X-Broker-Api-Version": "2.13",
            "Authorization": AUTH_HEADER,
            "unknown": "unknown",
        },
    )

    assert response.status_code == http.HTTPStatus.INTERNAL_SERVER_ERROR
    assert response.json() == dict(description=constants.DEFAULT_EXCEPTION_ERROR_MESSAGE)


def test_catalog_can_return_multiple_services(client, demo_service, mock_broker):
    mock_broker.catalog.return_value = [
        Service(
            id="s1",
            name="service_name1",
            description="service_description1",
            bindable=True,
            plans=[
                ServicePlan(id="p1", name="default1", description="plan_description1")
            ],
        ),
        Service(
            id="s2",
            name="service_name2",
            description="service_description2",
            bindable=True,
            plans=[
                ServicePlan(id="p2", name="default2", description="plan_description2")
            ],
        ),
    ]

    response = client.get(
        "/v2/catalog",
        headers={
            "X-Broker-Api-Version": "2.13",
            "Authorization": AUTH_HEADER,
            "unknown": "unknown",
        },
    )

    assert response.status_code == http.HTTPStatus.OK
    assert response.json() == dict(
        services=[
            dict(
                id="s1",
                name="service_name1",
                description="service_description1",
                bindable=True,
                instances_retrievable=False,
                bindings_retrievable=False,
                plan_updateable=False,
                plans=[
                    dict(id="p1", name="default1", description="plan_description1")
                ],
            ),
            dict(
                id="s2",
                name="service_name2",
                description="service_description2",
                bindable=True,
                instances_retrievable=False,
                bindings_retrievable=False,
                plan_updateable=False,
                plans=[
                    dict(id="p2", name="default2", description="plan_description2")
                ],
            ),
        ]
    )
