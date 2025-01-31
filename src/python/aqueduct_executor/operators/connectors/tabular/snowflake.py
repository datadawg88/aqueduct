from typing import Optional

from sqlalchemy import create_engine, engine

from aqueduct_executor.operators.connectors.tabular import config, models, relational


class SnowflakeConnector(relational.RelationalConnector):
    def __init__(self, config: config.SnowflakeConfig):
        conn_engine = _create_engine(config)
        super().__init__(conn_engine)


def _create_engine(config: config.SnowflakeConfig) -> engine.Engine:
    # Snowflake Dialect:
    # https://github.com/snowflakedb/snowflake-sqlalchemy
    url = "snowflake://{username}:{password}@{account_identifier}/{database}/{schema}?warehouse={warehouse}".format(
        username=config.username,
        password=config.password,
        account_identifier=config.account_identifier,
        database=config.database,
        schema=config.db_schema,
        warehouse=config.warehouse,
    )
    return create_engine(url)
