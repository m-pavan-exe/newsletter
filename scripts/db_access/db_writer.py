"""
Database writing module for the AI Summary Framework.

This module provides functions to write data to the database with proper
error handling, logging, and transaction management.
"""

from datetime import datetime, date
import secrets
import time
from typing import Any, Dict, Optional, Union

from sqlalchemy import text, exc
from sqlalchemy.engine import Engine

from utils import (
    get_logger,
    DatabaseError,
    handle_database_errors,
    handle_validation_errors
)

# Initialize logger
logger = get_logger(__name__)

@handle_validation_errors
def generate_unique_process_id() -> str:
    """
    Generate a unique process ID for database operations.

    Returns:
        str: A 10-digit string representing a unique process ID.

    Note:
        Falls back to a timestamp-based ID if cryptographic generation fails.
    """
    logger.debug("Generating unique process ID")
    try:
        # Generate a cryptographically secure random 10-digit number
        process_id = str(secrets.randbelow(10**10)).zfill(10)
        logger.info("Generated secure process ID: %s", process_id)
        return process_id
    except Exception as e:
        # Fallback to timestamp-based ID if cryptographic generation fails
        fallback_id = str(int(time.time()))[-10:].zfill(10)
        logger.warning(
            "Falling back to timestamp-based process ID due to error: %s. Fallback ID: %s",
            str(e), fallback_id
        )
        return fallback_id

@handle_database_errors
def insert_insight_summary(
    engine: Engine,
    country_param: str,
    le_book_param: str,
    business_date: Union[str, date],
    summary_content: str,
    pdf_file_name: str,
    table_name: str
) -> None:
    """
    Insert or update an insight summary in the database.

    This function performs an upsert operation (DELETE + INSERT) to ensure data consistency.

    Args:
        engine: SQLAlchemy engine instance
        country_param: Country code (e.g., 'US', 'UK')
        le_book_param: Legal entity book identifier
        business_date: Business date in 'YYYY-MM-DD' format or date object
        summary_content: The summary content to be stored
        pdf_file_name: Name of the associated PDF file
        table_name: Name of the table to insert the summary into
    Raises:
        DatabaseError: If there's an issue with the database operation
        ValueError: If the input parameters are invalid
    """
    logger.info(
        "Inserting insight summary for country=%s, le_book=%s, business_date=%s, file=%s",
        country_param, le_book_param, business_date, pdf_file_name
    )

    # Generate process ID
    process_id = generate_unique_process_id()
    logger.debug("Using process_id: %s", process_id)

    # Define the SQL for atomic upsert using SYSDATE for process_date_time
    insert_sql = text("""
        MERGE INTO {} t
        USING (
            SELECT :country         AS country,
                   :le_book         AS le_book,
                   TO_DATE(:business_date, 'YYYY-MM-DD') AS business_date,
                   :process_id      AS process_id,
                   :summary         AS summary,
                   SYSDATE          AS process_date_time,
                   :file_name       AS file_name
            FROM dual
        ) s
        ON (t.country = s.country AND
            t.le_book = s.le_book AND
            t.business_date = s.business_date)
        WHEN MATCHED THEN
            UPDATE SET
                process_id        = s.process_id,
                summary           = s.summary,
                process_date_time = s.process_date_time,
                file_name         = s.file_name
        WHEN NOT MATCHED THEN
            INSERT (country, le_book, business_date, process_id, summary, process_date_time, file_name)
            VALUES (s.country, s.le_book, s.business_date, s.process_id, s.summary, s.process_date_time, s.file_name)
    """.format(table_name))

    # Prepare parameters
    try:
        # Convert business_date to string in YYYY-MM-DD format if it's a date object
        if isinstance(business_date, date):
            business_date_str = business_date.strftime("%Y-%m-%d")
        elif isinstance(business_date, str):
            # Validate the string format
            try:
                datetime.strptime(business_date, "%Y-%m-%d")
                business_date_str = business_date
            except ValueError:
                raise ValueError("business_date must be in 'YYYY-MM-DD' format")
        else:
            raise ValueError("business_date must be a string in 'YYYY-MM-DD' format or a date object")

        params = {
            "country": country_param,
            "le_book": le_book_param,
            "business_date": business_date_str,  # Pass as string, let Oracle convert
            "process_id": process_id,
            "summary": summary_content,
            "file_name": pdf_file_name
        }

        logger.debug("Executing upsert operation with parameters")
        
        # Execute within a transaction
        with engine.begin() as conn:
            # Log the operation for audit purposes
            logger.debug("Executing SQL: %s", str(insert_sql))
            result = conn.execute(insert_sql, params)
            logger.debug("Operation affected %s rows", result.rowcount)
            
        logger.info("Successfully upserted insight summary")
        
    except exc.SQLAlchemyError as e:
        error_msg = "Failed to upsert insight summary"
        logger.exception("%s: %s", error_msg, str(e))
        raise DatabaseError(
            message=error_msg,
            details={
                "country": country_param,
                "le_book": le_book_param,
                "business_date": str(business_date),
                "error": str(e)
            }
        ) from e
    except ValueError as e:
        error_msg = "Invalid input parameters"
        logger.error("%s: %s", error_msg, str(e))
        raise ValueError(f"{error_msg}: {str(e)}") from e
