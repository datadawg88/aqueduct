import time
from typing import Dict, Optional, List, Union
import aqueduct
import uuid

from aqueduct import Flow
from aqueduct.check_artifact import CheckArtifact
from aqueduct.generic_artifact import Artifact
from aqueduct.metric_artifact import MetricArtifact
from aqueduct.param_artifact import ParamArtifact
from aqueduct.table_artifact import TableArtifact

# Should be set before each test runs.
from test_functions.sentiment.model import sentiment_model, sentiment_model_multiple_input
from test_functions.simple.model import dummy_sentiment_model, dummy_sentiment_model_multiple_input

flags: Dict[str, bool] = {}
integration_name: Optional[str] = None


def get_integration_name() -> str:
    assert integration_name is not None
    return integration_name


def should_publish_flows() -> bool:
    assert "publish" in flags
    return flags["publish"]


def should_run_complex_models() -> bool:
    assert "complex_models" in flags
    return flags["complex_models"]


def generate_new_flow_name() -> str:
    return "workflow_" + uuid.uuid4().hex[:8]


def generate_table_name() -> str:
    return "test_table_" + uuid.uuid4().hex[:8]


def run_sentiment_model(artifact: TableArtifact) -> TableArtifact:
    """
    Calls the full sentiment model if --complex_models flag is set. Otherwise, will use simple model,
    which appends the same column with a dummy value, but is much faster.
    """
    if should_run_complex_models():
        return sentiment_model(artifact)
    else:
        return dummy_sentiment_model(artifact)


def run_sentiment_model_multiple_input(
    artifact1: TableArtifact, artifact2: TableArtifact
) -> TableArtifact:
    """
    Same test setup as `run_sentiment_model`.
    """
    if should_run_complex_models():
        return sentiment_model_multiple_input(artifact1, artifact2)
    else:
        return dummy_sentiment_model_multiple_input(artifact1, artifact2)


def run_flow_test(
    client: aqueduct.Client,
    artifacts: List[Union[TableArtifact, MetricArtifact, CheckArtifact, ParamArtifact]],
    name: str = "",
    schedule: str = "",
    num_runs: int = 1,
    delete_flow_after: bool = True,
) -> Optional[Flow]:
    """
    Actually publishes the flow if tests are run with --publish flag. This flow can be deleted
    within this method if `delete_flow_after = True`.

    If --publish is not supplied, we will instead realize all the artifacts with .get().
    The --publish case only returns when the specified flow has run successfully at least `num_runs` times.
    """
    if not should_publish_flows():
        for artifact in artifacts:
            _ = artifact.get()
        return None

    if len(name) == 0:
        name = generate_new_flow_name()

    flow = client.publish_flow(
        name=name,
        artifacts=artifacts,
        schedule=schedule,
    )
    print("Workflow registration succeeded. Workflow ID: %s" % flow.id())

    try:
        wait_for_flow_runs(client, flow.id(), num_runs)
    finally:
        if delete_flow_after:
            delete_flow(client, flow.id())
    return flow


def wait_for_flow_runs(sp_client: aqueduct.Client, flow_id: uuid.UUID, num_runs: int = 1) -> int:
    """
    Returns only when the specified flow has run successfully at least `num_runs` times.
    Any run failure is not tolerated. Will timeout after a few minutes.

    Returns:
        The number of successful runs this flow has performed.
    """
    timeout = 300
    poll_threshold = 5
    begin = time.time()

    while True:
        assert time.time() - begin < timeout, "Timed out waiting for workflow run to complete."

        time.sleep(poll_threshold)
        flow_resp = sp_client._get_flow_info(str(flow_id))

        # A flow has been successfully published if it makes at least one workflow run, and
        # all its workflow runs have executed successfully.
        results = flow_resp["workflow_dag_results"]
        if len(results) < num_runs:
            continue

        assert all(
            result["status"] != "failed" for result in results
        ), "At least one workflow run failed!"

        # Continue checking as long as there are still runs pending.
        if any(result["status"] == "pending" for result in results):
            continue

        print(
            "Workflow %s was created and ran successfully at least %s times!" % (flow_id, num_runs)
        )
        return len(results)
    return -1


def delete_flow(sp_client: aqueduct.Client, workflow_id: uuid.UUID) -> None:
    try:
        sp_client.delete_flow(str(workflow_id))
    except Exception as e:
        print("Error deleting workflow %s with exception %s" % (workflow_id, e))
    else:
        print("Successfully deleted workflow %s" % (workflow_id))
