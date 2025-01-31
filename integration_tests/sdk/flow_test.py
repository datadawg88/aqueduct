import pytest
import aqueduct
from test_metrics.constant.model import constant_metric
from aqueduct import LoadUpdateMode, check
from aqueduct.error import IncompleteFlowException
from datetime import datetime, timedelta

from constants import SENTIMENT_SQL_QUERY
from test_functions.simple.model import dummy_model
from utils import (
    generate_new_flow_name,
    get_integration_name,
    run_flow_test,
    run_sentiment_model_multiple_input,
    run_sentiment_model,
    generate_table_name,
    delete_flow,
    wait_for_flow_runs,
)


def test_basic_flow(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_artifact = run_sentiment_model(sql_artifact)
    output_artifact.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )

    run_flow_test(sp_client, artifacts=[output_artifact])


def test_complex_flow(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact1 = db.sql(name="Query 1", query=SENTIMENT_SQL_QUERY)
    sql_artifact2 = db.sql(name="Query 2", query=SENTIMENT_SQL_QUERY)

    fn_artifact = run_sentiment_model_multiple_input(sql_artifact1, sql_artifact2)
    output_artifact = dummy_model(fn_artifact)
    output_artifact.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )

    @check()
    def successful_check(df):
        return True

    @check()
    def failing_check(df):
        return False

    run_flow_test(
        sp_client,
        artifacts=[
            output_artifact,
            successful_check(output_artifact),
            failing_check(output_artifact),
        ],
    )


def test_multiple_output_artifacts(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact1 = db.sql(name="Query 1", query=SENTIMENT_SQL_QUERY)
    sql_artifact2 = db.sql(name="Query 2", query=SENTIMENT_SQL_QUERY)

    fn_artifact1 = run_sentiment_model(sql_artifact1)
    fn_artifact2 = dummy_model(sql_artifact2)
    fn_artifact1.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )
    fn_artifact2.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )

    run_flow_test(
        sp_client,
        artifacts=[fn_artifact1, fn_artifact2],
    )


@pytest.mark.publish
def test_publish_with_schedule(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_artifact = run_sentiment_model(sql_artifact)
    output_artifact.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )

    # Execute the flow 1 minute from now.
    execute_at = datetime.now() + timedelta(minutes=1)
    run_flow_test(
        sp_client,
        artifacts=[output_artifact],
        schedule=aqueduct.hourly(minute=aqueduct.Minute(execute_at.minute)),
    )


def test_invalid_flow(sp_client):
    with pytest.raises(IncompleteFlowException):
        sp_client.publish_flow(
            name=generate_new_flow_name(),
            artifacts=[],
        )

    with pytest.raises(Exception):
        sp_client.publish_flow(
            name=generate_new_flow_name(),
            artifacts=["123"],
        )


@pytest.mark.publish
def test_publish_flow_with_same_name(sp_client):
    """Tests flow editing behavior."""
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_artifact = run_sentiment_model(sql_artifact)

    # Remember to cleanup any created test data.
    flow_ids_to_delete = set()
    try:
        flow_name = generate_new_flow_name()
        flow = run_flow_test(
            sp_client,
            artifacts=[output_artifact],
            name=flow_name,
            schedule=aqueduct.daily(),
            delete_flow_after=False,
        )
        flow_ids_to_delete.add(flow.id())

        # Add a metric to the flow and re-publish under the same name.
        metric = constant_metric(output_artifact)
        flow = run_flow_test(
            sp_client,
            artifacts=[metric],
            name=flow_name,
            schedule=aqueduct.daily(hour=aqueduct.Hour(1)),
            delete_flow_after=False,
        )
        flow_ids_to_delete.add(flow.id())
    finally:
        for flow_id in flow_ids_to_delete:
            delete_flow(sp_client, flow_id)


@pytest.mark.publish
def test_refresh_flow(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_artifact = run_sentiment_model(sql_artifact)
    output_artifact.save(
        config=db.config(table=generate_table_name(), update_mode=LoadUpdateMode.REPLACE)
    )
    flow = sp_client.publish_flow(
        name=generate_new_flow_name(),
        artifacts=[output_artifact],
        schedule=aqueduct.hourly(),
    )

    # Wait for the first run, then refresh the workflow and verify that it runs at least
    # one more time.
    try:
        num_initial_runs = wait_for_flow_runs(sp_client, flow.id())
        sp_client.trigger(flow.id())
        wait_for_flow_runs(sp_client, flow.id(), num_runs=num_initial_runs + 1)
    finally:
        sp_client.delete_flow(flow.id())
