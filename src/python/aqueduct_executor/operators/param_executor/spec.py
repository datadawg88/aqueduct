import json
from pydantic import BaseModel, parse_obj_as

try:
    from typing import Literal
except ImportError:
    # Python 3.7 does not support typing.Literal
    from typing_extensions import Literal

from aqueduct_executor.operators.utils import enums
from aqueduct_executor.operators.utils.storage import config


class ParamSpec(BaseModel):
    name: str
    type: Literal[enums.JobType.PARAM]
    storage_config: config.StorageConfig
    metadata_path: str
    val: str
    output_content_path: str
    output_metadata_path: str


def parse_spec(spec_json: str) -> ParamSpec:
    """
    Parses a JSON string into a FunctionSpec.
    """
    data = json.loads(spec_json)
    return parse_obj_as(ParamSpec, data)
