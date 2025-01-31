from typing import Dict, List

import pytest

from constants import SENTIMENT_SQL_QUERY
from utils import get_integration_name, run_flow_test, wait_for_flow_runs
from aqueduct import metric, op
import pandas as pd


@metric
def double_number_input(num: int) -> float:
    if not isinstance(num, int):
        raise Exception("Expected an integer input.")
    return float(2 * num)


@metric
def len_of_word(word: str) -> int:
    if not isinstance(word, str):
        raise Exception("Expected a string input.")
    return len(word)


@op
def convert_dict_to_df(kv: Dict[str, List[float]]):
    return pd.DataFrame(data=kv)


def test_basic_param_creation(sp_client):
    # Parameter of integer type
    param = sp_client.create_param(name="number", default=8)
    assert param.get() == 8

    param_doubled = double_number_input(param)
    assert param_doubled.get() == 2 * 8

    # Parameter of string type
    param = sp_client.create_param(name="word", default="hello world")
    assert param.get() == "hello world"

    param_length = len_of_word(param)
    assert param_length.get() == len("hello world")

    # Parameter of dictionary type
    kv = {"col 1": [1.23, 4.56], "col 2": [7.89, 1.23]}
    param = sp_client.create_param(name="word", default=kv)
    assert param.get() == kv

    kv_df = convert_dict_to_df(param)
    assert kv_df.get().equals(pd.DataFrame(data=kv))


@op
def append_row_to_df(df, row):
    """`row` is a list of values to append to the input dataframe."""
    df.loc[len(df.index)] = row
    return df


def test_parameter_in_basic_flow(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    row_to_add = ["new hotel", "09-28-1996", "US", "It was new."]
    new_row_param = sp_client.create_param(name="new row", default=row_to_add)
    output = append_row_to_df(sql_artifact, new_row_param)

    input_df = sql_artifact.get()
    input_df.loc[len(input_df.index)] = row_to_add

    output_df = output.get()
    assert output_df.equals(input_df)


@pytest.mark.publish
def test_edit_param_for_flow(sp_client):
    db = sp_client.integration(name=get_integration_name())
    sql_artifact = db.sql(query=SENTIMENT_SQL_QUERY)
    row_to_add = ["new hotel", "09-28-1996", "US", "It was new."]
    new_row_param = sp_client.create_param(name="new row", default=row_to_add)
    output = append_row_to_df(sql_artifact, new_row_param)

    flow_name = "Edit Parameter Test Flow"
    flow = run_flow_test(sp_client, artifacts=[output], name=flow_name, delete_flow_after=False)
    flow_id = flow.id()

    try:
        # Edit the flow with a different row to append and re-publish
        row_to_add = ["another new hotel", "10-10-1000", "ID", "It was really really new."]
        new_row_param = sp_client.create_param(name="new row", default=row_to_add)
        output = append_row_to_df(sql_artifact, new_row_param)

        # Wait for the first run, then refresh the workflow and verify that it runs at least
        # one more time (two runs total, since the original was manually triggered).
        flow = run_flow_test(
            sp_client, artifacts=[output], name=flow_name, num_runs=2, delete_flow_after=True
        )
    except Exception:
        sp_client.delete_flow(flow.id())
        raise

    assert flow_id == flow.id()
