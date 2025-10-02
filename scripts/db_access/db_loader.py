"""
Data ingestion module for loading data from a SQL database into a pandas DataFrame.

This module provides functions to connect to databases, execute queries, and fetch data
in a consistent and reliable manner with proper error handling and logging.
"""

import json
from typing import Any, Dict, Optional, List, Tuple, Union

import pandas as pd
from sqlalchemy import create_engine, text, exc
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import sessionmaker, scoped_session

from utils import (
    get_logger,
    DatabaseError,
    ConfigurationError,
    handle_database_errors,
    handle_validation_errors
)

# Initialize logger
logger = get_logger(__name__)

# Create a thread-local session factory
Session = scoped_session(sessionmaker())

@handle_database_errors
def fetch_book_ccy(engine: Engine, country: str) -> str:
    """
    Fetch BOOK_CCY (currency type) for the given country from LE_BOOK table.

    Args:
        engine: SQLAlchemy engine instance
        country: Country code to look up

    Returns:
        str: Currency code (e.g., 'USD', 'EUR') or empty string if not found

    Raises:
        DatabaseError: If there's an issue executing the query
    """
    logger.info("Fetching BOOK_CCY for country: %s", country)
    
    query = text("""
        SELECT BOOK_CCY 
        FROM LE_BOOK 
        WHERE country = :country
    """)
    
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {"country": country})
            row = result.fetchone()
            logger.debug("Fetched BOOK_CCY result: %s", row)
            return row[0] if row else ""
    except exc.SQLAlchemyError as e:
        error_msg = f"Failed to fetch BOOK_CCY for country {country}"
        logger.exception(error_msg)
        raise DatabaseError(
            message=error_msg,
            details={"country": country, "error": str(e)}
        ) from e


def create_sqlalchemy_mssql_engine(config: Dict[str, str]) -> Engine:
    try:
        conn_str = f"mssql+pyodbc://{config['username']}:{config['password']}@{config['server']}/{config['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&Encrypt=no"
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        logger.error(f"Error creating SQLAlchemy engine: {e} ")
        raise DatabaseError(
            message="Failed to create SQLAlchemy engine",
            details={"error": str(e)}
        ) from e



@handle_validation_errors
def create_sqlalchemy_oracle_engine(config: Dict[str, Any]) -> Engine:
    """
    Backwards-compatible wrapper that creates a SQLAlchemy Oracle engine.

    Historically this function produced a SQL Server engine, but the
    application now supports Oracle.  To avoid duplicating logic, we simply
    delegate to ``create_oracle_engine`` which contains the full, validated
    implementation for establishing an Oracle connection (see lines 154-233).

    The *config* dict must contain at least ``username`` and ``password`` and
    either a ``dsn`` **or** the trio ``host``, ``port`` (optional, defaults to
    ``1521``) and ``service_name``.  Additional pooling options are handled in
    ``create_oracle_engine``.
    """
    # Re-use the fully-featured Oracle engine builder below.
    return create_oracle_engine(config)


@handle_validation_errors
def create_oracle_engine(config: Dict[str, Any]) -> Engine:
    """
    Create and return a SQLAlchemy engine for Oracle database using oracledb.

    Args:
        config: Dictionary containing Oracle database configuration with keys:
               - username: Database username
               - password: Database password
               - host: Database server hostname
               - port: Database port (default: 1521)
               - service_name: Oracle service name
               - dsn: Optional DSN string (alternative to host/port/service_name)

    Returns:
        Engine: Configured SQLAlchemy engine for Oracle

    Raises:
        ConfigurationError: If required config values are missing
        DatabaseError: If engine creation fails
    """
    required_keys = {'username', 'password'}
    missing_keys = required_keys - set(config.keys())
    
    if missing_keys:
        error_msg = f"Missing required Oracle configuration keys: {', '.join(missing_keys)}"
        logger.error(error_msg)
        raise ConfigurationError(
            message=error_msg,
            details={"missing_keys": list(missing_keys)}
        )
        
    # Check if either DSN or host/port/service_name is provided
    if 'dsn' not in config and not all(k in config for k in ['host', 'service_name']):
        error_msg = "Either 'dsn' or 'host' and 'service_name' must be provided in config"
        logger.error(error_msg)
        raise ConfigurationError(
            message=error_msg,
            details={"config_keys": list(config.keys())}
        )
    
    try:
        # Use DSN if provided, otherwise construct connection string
        if 'dsn' in config:
            dsn = config['dsn']
            logger.info("Creating Oracle engine using provided DSN")
        else:
            host = config.get('host', 'localhost')
            port = str(config.get('port', '1521'))
            service_name = config['service_name']
            dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))(CONNECT_DATA=(SERVICE_NAME={service_name})))"
            logger.info("Creating Oracle engine for host: %s, service: %s", host, service_name)
        
        # Create connection URL
        conn_url = URL.create(
            "oracle+oracledb",
            username=config['username'],
            password=config['password'],
            query={"dsn": dsn}
        )
        
        # Create engine with connection pooling
        engine = create_engine(
            conn_url,
            pool_pre_ping=True,
            pool_recycle=1800,  # Recycle connections after 30 minutes
            max_overflow=10,     # Allow 10 connections beyond pool_size
            pool_size=5,        # Maintain 5 connections in the pool
            pool_timeout=30     # Wait up to 30 seconds for a connection
        )
        
        # Test the connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1 FROM DUAL"))
            
        logger.info("Oracle engine created and connection tested successfully")
        return engine
        
    except exc.SQLAlchemyError as e:
        error_msg = "Failed to create Oracle engine"
        logger.exception("%s: %s", error_msg, str(e))
        raise DatabaseError(
            message=error_msg,
            details={
                "host": config.get('host'),
                "service_name": config.get('service_name'),
                "error": str(e)
            }
        ) from e



def get_data_for_summary(engine, country: str, le_book: str, business_date: Optional[str] = None) -> pd.DataFrame:
    logger.info("Loading data for summary...")
    filters = {"COUNTRY": country, "LE_BOOK": le_book, "BUSINESS_DATE": business_date}
    df = fetch_dataframe(engine, "DM_RA_DETAILS_V2", filters=filters)
    logger.info(f"Data loaded for summary. DataFrame shape: {df.shape}")
    return df

def fetch_dataframe(
    engine,
    table: str,
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[list] = None,
) -> pd.DataFrame:
    """
    Fetch data from a database table into a pandas DataFrame.
    
    Args:
        engine: SQLAlchemy engine
        table (str): Name of the table to query
        filters (dict, optional): Dictionary of column-value pairs to filter by
        columns (list, optional): List of columns to select. If None, selects all columns.
    
    Returns:
        pd.DataFrame: DataFrame containing the query results with uppercase column names
    """
    logger.info(f"Fetching DataFrame from table {table}...")
    try:
        filters = filters or {}
        # Convert column names to uppercase for the SQL query
        columns_sql = ", ".join([col.upper() for col in columns]) if columns else "*"
        
        # Build the WHERE clause with proper date formatting
        where_clauses = []
        params = {}
        for col, val in filters.items():
            if val is not None:  # Skip None values
                col_upper = col.upper()
                if col_upper == 'BUSINESS_DATE' and isinstance(val, str):
                    # Handle date-only comparison differently based on the database dialect
                    dialect = engine.dialect.name.lower()
                    if dialect.startswith("oracle"):
                        # Oracle: use TRUNC + TO_DATE
                        where_clauses.append(f"TRUNC({col_upper}) = TO_DATE(:{col}, 'YYYY-MM-DD')")
                    else:
                        # SQL Server (and others): cast both sides to DATE
                        where_clauses.append(f"CAST({col_upper} AS DATE) = :{col}")
                else:
                    where_clauses.append(f"{col_upper} = :{col}")
                params[col] = val
        
        where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        sql = f"SELECT {columns_sql} FROM {table} {where_clause}"
        
        logger.debug(f"Executing SQL: {sql}")
        logger.debug(f"With parameters: {params}")
        
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        
        # Convert column names to uppercase
        df.columns = [col.upper() for col in df.columns]
        logger.info(f"Successfully fetched {len(df)} rows from {table}")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching DataFrame from {table}: {e}")
        raise

def execute_custom_query(engine, sql_query: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Execute a custom SQL query and return the results as a pandas DataFrame.
    
    Args:
        engine: SQLAlchemy engine
        sql_query (str): The SQL query to execute
        params (dict, optional): Dictionary of parameters for the query
        
    Returns:
        pd.DataFrame: DataFrame containing the query results
        
    Raises:
        ValueError: If no rows are returned by the query
        Exception: For any database errors
    """
    logger.info("Executing custom SQL query...")
    logger.debug(f"Query: {sql_query}")
    if params:
        logger.debug(f"Parameters: {params}")
        
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql_query), params or {})
            rows = result.fetchall()
            
            if not rows:
                logger.warning("Query returned no rows")
                return pd.DataFrame()
                
            # Get column names from result set
            columns = [col.upper() for col in result.keys()]
            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"Query executed successfully. Returned {len(df)} rows")
            return df
            
    except Exception as e:
        error_msg = f"Error executing custom query: {str(e)}"
        logger.error(error_msg, exc_info=True)
        raise Exception(error_msg) from e


def load_db_config_from_json(json_path: str = "db_config.json") -> Dict[str, str]:
    logger.info(f"Loading DB config from JSON file: {json_path}")
    with open(json_path, "r") as f:
        config = json.load(f)
    logger.debug(f"DB config loaded: keys={list(config.keys())}")
    return {
        "server": config["DB_SERVER"],
        "database": config["DB_NAME"],
        "username": config["DB_USER"],
        "password": config["DB_PASSWORD"]
    }





def fetch_required_vision_variables(engine, variable_mapping):
    """
    Fetch required variables from Vision_Variables table.
    
    Args:
        engine: SQLAlchemy engine
        variable_mapping (dict or list): Either a dictionary mapping database variable names to output keys,
                                     or a list of variable names (for backward compatibility).
                                     Example as dict: {'RA_BS_SUMMARY_TEMPLATE': 'template_path'}
                                     Example as list: ['RA_BS_SUMMARY_TEMPLATE']
    
    Returns:
        dict: Dictionary containing the requested variables with the specified keys.
               If input was a list, keys will be the same as the variable names.
    """
    # Handle both list and dictionary inputs
    if isinstance(variable_mapping, list):
        # For backward compatibility, use variable names as both keys and values
        variable_names = variable_mapping
        output_mapping = {name: name for name in variable_names}
    else:
        # New behavior with custom output keys
        output_mapping = variable_mapping
        variable_names = list(output_mapping.keys())
        
    logger.info(f"Fetching required Vision_Variables: {', '.join(variable_names)}")

    placeholders = ','.join([f":var{i}" for i in range(len(variable_names))])
    sql = f"""
        SELECT VARIABLE, VALUE
        FROM Vision_Variables
        WHERE VARIABLE IN ({placeholders})
    """
    params = {f"var{i}": name for i, name in enumerate(variable_names)}
    
    logger.debug(f"Vision_Variables SQL: {sql}, params: {params}")
    
    with engine.connect() as connection:
        result = connection.execute(text(sql), params)
        rows = result.fetchall()
        logger.debug(f"Vision_Variables fetched rows: {rows}")
        
        if not rows or len(rows) < len(variable_names):
            found_vars = {row[0] for row in rows}
            missing_vars = set(variable_names) - found_vars
            error_msg = f"Missing required Vision_Variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        vision_vars = {row[0]: row[1] for row in rows}
    
    # Map the database variables to the requested output keys
    result = {}
    for var_name, output_key in output_mapping.items():
        if var_name in vision_vars:
            result[output_key] = vision_vars[var_name]
    
    logger.info(f"Successfully loaded {len(result)}/{len(variable_names)} variables")
    return result
