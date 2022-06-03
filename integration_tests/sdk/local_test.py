from doctest import OutputChecker
from pandas import DataFrame
import pytest
import aqueduct
from test_metrics.constant.model import constant_metric
from test_functions.simple.model import dummy_sentiment_model, dummy_sentiment_model_multiple_input
from test_functions.sentiment.model import sentiment_model
from checks_test import success_on_single_table_input
from constants import SENTIMENT_SQL_QUERY
from utils import (
    get_integration_name,
    run_sentiment_model,
    should_run_complex_models
)

def test_local_operator(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_artifact = run_sentiment_model(sql_artifact)
    output_cloud = output_artifact.get()
    output_local = sentiment_model.local(sql_artifact) if should_run_complex_models() else dummy_sentiment_model.local(sql_artifact)
    if should_run_complex_models():
        assert output_cloud.count()[0] == output_local.count()[0]
        assert (output_cloud["positivity"] == output_local["positivity"]).all()
    else:
        assert output_cloud.count()[0] == output_local.count()[0]
        assert (output_cloud["positivity"] == output_local["positivity"]).all()

def test_local_metric(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)

    metric = constant_metric(sql_artifact)
    assert metric.get() == 17.5
    assert constant_metric.local(sql_artifact) == 17.5

def test_local_check(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    check = success_on_single_table_input
    assert check(sql_artifact)
    assert check.local(sql_artifact)

def test_local_dataframe_input(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    output_local = dummy_sentiment_model.local(sql_artifact.get())
    assert type(output_local) is DataFrame

def test_local_on_multiple_inputs(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    sql_artifact2 = db.sql(query=SENTIMENT_SQL_QUERY)
    output_local = dummy_sentiment_model_multiple_input.local(sql_artifact,sql_artifact2)
    assert type(output_local) is DataFrame