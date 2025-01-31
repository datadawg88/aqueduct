"""
CONFIG FILE FOR TABULAR CONNECTOR INTEGRATION TESTS
- To skip a particular test set the relevant `SKIP_` flag to False
- Set the `_CONF` dict for all connectors being tested.
- Commented out config fields are optional.
"""
from aqueduct_executor.operators.connectors.tabular import postgres
from aqueduct_executor.operators.connectors.tabular import snowflake
from aqueduct_executor.operators.connectors.tabular import mysql
from aqueduct_executor.operators.connectors.tabular import sql_server
from aqueduct_executor.operators.connectors.tabular import bigquery
from aqueduct_executor.operators.connectors.tabular import sqlite

"""FLAGS TO SKIP TESTS"""
SKIP_POSTGRES = True
SKIP_SNOWFLAKE = True
SKIP_MYSQL = True
SKIP_REDSHIFT = True
SKIP_MARIADB = True
SKIP_SQL_SERVER = True
SKIP_BIGQUERY = True
SKIP_SQLITE = True

"""POSTGRES CONFIG"""
POSTGRES_CONF = {
    postgres._CONFIG_USERNAME_KEY: "",
    postgres._CONFIG_PASSWORD_KEY: "",
    postgres._CONFIG_DATABASE_KEY: "",
    postgres._CONFIG_HOST_KEY: "",
    # postgres._CONFIG_PORT_KEY: "",
}

"""SNOWFLAKE CONFIG"""
SNOWFLAKE_CONF = {
    snowflake._CONFIG_USERNAME_KEY: "",
    snowflake._CONFIG_PASSWORD_KEY: "",
    snowflake._CONFIG_ACCOUNT_IDENTIFIER_KEY: "",
    snowflake._CONFIG_DATABASE_KEY: "",
    snowflake._CONFIG_WAREHOUSE_KEY: "",
    # snowflake._CONFIG_SCHEMA_KEY: "",
}

"""MYSQL CONFIG"""
MYSQL_CONF = {
    mysql._CONFIG_USERNAME_KEY: "",
    mysql._CONFIG_PASSWORD_KEY: "",
    mysql._CONFIG_DATABASE_KEY: "",
    mysql._CONFIG_HOST_KEY: "",
    mysql._CONFIG_PORT_KEY: "",
}

"""REDSHIFT CONFIG"""
REDSHIFT_CONF = {
    postgres._CONFIG_USERNAME_KEY: "",
    postgres._CONFIG_PASSWORD_KEY: "",
    postgres._CONFIG_DATABASE_KEY: "",
    postgres._CONFIG_HOST_KEY: "",
    postgres._CONFIG_PORT_KEY: "",
}

"""MARIADB CONFIG"""
MARIADB_CONF = {
    mysql._CONFIG_USERNAME_KEY: "",
    mysql._CONFIG_PASSWORD_KEY: "",
    mysql._CONFIG_DATABASE_KEY: "",
    mysql._CONFIG_HOST_KEY: "",
    mysql._CONFIG_PORT_KEY: "",
}

"""SQL SERVER CONFIG"""
SQL_SERVER_CONF = {
    sql_server._CONFIG_USERNAME_KEY: "",
    sql_server._CONFIG_PASSWORD_KEY: "",
    sql_server._CONFIG_DATABASE_KEY: "",
    sql_server._CONFIG_HOST_KEY: "",
    sql_server._CONFIG_PORT_KEY: "",
}

"""BIGQUERY CONFIG"""
BIGQUERY_CONF = {
    bigquery._CONFIG_PROJECT_ID_KEY: "",
    bigquery._CONFIG_SERVICE_ACCT_CREDENTIALS_KEY: "",
}

"""SQLITE CONFIG"""
SQLITE_CONF = {
    sqlite._CONFIG_DATABASE_PATH_KEY: "",
}
