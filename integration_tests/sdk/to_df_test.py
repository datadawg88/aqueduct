import pytest
from aqueduct.error import (
    AqueductError,
    InvalidFunctionException,
    InvalidDependencyFilePath,
)

from constants import (
    SENTIMENT_SQL_QUERY,
)
from test_functions.noop_with_requirements.model import noop_model_with_requirements_file
from test_functions.noop_without_requirements.model import noop_model_without_requirements_file
from test_functions.sentiment_without_requirements.model import sentiment_model_without_requirements
from test_functions.simple.file_dependency_model import (
    model_with_file_dependency,
    model_with_invalid_dependencies,
    model_with_missing_file_dependencies,
    model_with_improper_dependency_path,
    model_with_out_of_package_file_dependency,
)
from test_functions.simple.model import dummy_model
from utils import (
    get_integration_name,
    run_sentiment_model,
    run_sentiment_model_multiple_input,
)


def test_basic_get(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    sql_df = sql_artifact.get()
    assert list(sql_df) == ["hotel_name", "review_date", "reviewer_nationality", "review"]
    assert sql_df.shape[0] == 100

    output_artifact = run_sentiment_model(sql_artifact)
    output_df = output_artifact.get()
    assert list(output_df) == [
        "hotel_name",
        "review_date",
        "reviewer_nationality",
        "review",
        "positivity",
    ]
    assert output_df.shape[0] == 100


def test_complex_get(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact1 = db.sql(name="Query 1", query=SENTIMENT_SQL_QUERY)
    sql_artifact2 = db.sql(name="Query 2", query=SENTIMENT_SQL_QUERY)

    fn_artifact = run_sentiment_model_multiple_input(sql_artifact1, sql_artifact2)
    fn_df = fn_artifact.get()

    assert list(fn_df) == [
        "hotel_name",
        "review_date",
        "reviewer_nationality",
        "review",
        "positivity",
        "positivity_2",
    ]
    assert fn_df.shape[0] == 100

    output_artifact = dummy_model(fn_artifact)
    output_df = output_artifact.get()
    assert list(output_df) == [
        "hotel_name",
        "review_date",
        "reviewer_nationality",
        "review",
        "positivity",
        "positivity_2",
        "newcol",
    ]
    assert fn_df.shape[0] == 100


def test_basic_file_dependencies(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)

    output_artifact = model_with_file_dependency(sql_artifact)
    output_df = output_artifact.get()
    assert list(output_df) == [
        "hotel_name",
        "review_date",
        "reviewer_nationality",
        "review",
        "newcol",
    ]
    assert output_df.shape[0] == 100


def test_invalid_file_dependencies(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)

    output_artifact = model_with_invalid_dependencies(sql_artifact)
    with pytest.raises(AqueductError):
        output_artifact.get()

    output_artifact = model_with_missing_file_dependencies(sql_artifact)
    with pytest.raises(AqueductError):
        output_artifact.get()

    # This one is caught early by the SDK because it's doesn't require execution.
    with pytest.raises(InvalidFunctionException):
        model_with_improper_dependency_path(sql_artifact)

    with pytest.raises(InvalidDependencyFilePath):
        model_with_out_of_package_file_dependency(sql_artifact)
