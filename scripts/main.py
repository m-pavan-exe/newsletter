"""
CLI entry point for the AI Summary Framework.
vllm serve Qwen/Qwen3-4B --host 0.0.0.0 --port 8000 --max-model-len 8000

"""
import argparse
import os
import logging
import sys
from pipeline import run_ra_summary_pipeline,run_bs_summary_pipeline
from db_access.db_loader import create_sqlalchemy_oracle_engine, create_sqlalchemy_mssql_engine, fetch_required_vision_variables
from utils.config_loader import load_full_config_from_json
from datetime import datetime
import traceback
from utils.logging_config import setup_logging
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None) 
pd.set_option('display.float_format', '{:,.2f}'.format)
# pd.set_option('display.max_rows', None)

# Initialize logger
logger = logging.getLogger(__name__)

# Remove any pre-existing handlers associated with the root logger to avoid duplicates
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# Initialize logging immediately so that any errors during start-up are captured
# Logs will be written to the default "logs" directory until a pipeline-specific
# directory is determined later.
logger.info("Application started")

def main():
    """Main entry point for the AI Summary Framework"""
    logger.info("Starting pipeline execution")
    try:
        if len(sys.argv) > 1 and any(arg.startswith("--") for arg in sys.argv):
            # Proper command-line execution
            parser = argparse.ArgumentParser(description="Run AI Summary Pipeline")
            parser.add_argument('--country', type=str, required=True, help="Country code (e.g., 'KE')")
            parser.add_argument('--le_book', type=str, required=True, help="Ledger book code (e.g., '01')")
            parser.add_argument('--business_date', type=str, help="Business date (e.g., '2024-01-01')")
            parser.add_argument('--pipeline_type', type=str, required=True, choices=['ra_summary', 'bs_summary'], help="Pipeline type (ra_summary/bs_summary)")
            args = parser.parse_args()

            country = args.country
            le_book = args.le_book
            business_date = args.business_date
            pipeline_type = args.pipeline_type
        else:
            raise ValueError("No command-line arguments found.")

    except (SystemExit, ValueError) as e:
        # Handle both argparse failure and fallback logic
        logger.warning(f"Falling back to default parameters: {e}")
        country = "KE"
        le_book = "01"
        business_date = "2025-04-30"
        pipeline_type = "ra_summary"

    try:
        # Load full config
        config_path = "/home/vision/app/NewsLetter/config.json"
        logger.info(f"Loading configuration from: {config_path}")
        config = load_full_config_from_json(config_path)
        logger.info(f"Configuration loaded successfully")
        
        # Create DB engine
        logger.info("Creating database engine")
        engine = create_sqlalchemy_oracle_engine(config)
        logger.info("Database engine created successfully")
    except Exception as e:
        logger.error(f"Failed to load configuration or create DB engine: {str(e)}", exc_info=True)
        sys.exit(1)

    # Fetch template_path, log_dir, output_dir from Vision_Variables
    try:
        if pipeline_type == "bs_summary":
            output_dir= r"/home/vision/app/NewsLetter/outputs"
            log_dir = r"/home/vision/app/NewsLetter/logs"
            template_path = r"//home/vision/app/NewsLetter/template/BS_AI_SUMMARY_TEMPLATE.docx"
            # Uncomment the following lines if you want to fetch these
            # variable_names = [
        #      'BS_AI_SUMMARY_DATAPATH',
            #     'BS_AI_SUMMARY_LOGPATH',
            #     'BS_AI_SUMMARY_TEMPLATE'
            # ]
            # paths = fetch_required_vision_variables(engine, variable_names)
            # template_path = paths["BS_AI_SUMMARY_TEMPLATE"]
            # log_dir = paths["BS_AI_SUMMARY_LOGPATH"]
            # output_dir = paths["BS_AI_SUMMARY_DATAPATH"]
        elif pipeline_type == "ra_summary":
            output_dir= r"/home/vision/app/NewsLetter/outputs"
            log_dir = r"/home/vision/app/NewsLetter/logs"
            template_path = r"/home/vision/app/NewsLetter/template/RA_AI_SUMMARY_TEMPLATE.docx"

            # variable_names = [
            #     'RA_AI_SUMMARY_DATAPATH',
            #     'RA_AI_SUMMARY_LOGPATH',
            #     'RA_AI_SUMMARY_TEMPLATE'
            # ]
            # paths = fetch_required_vision_variables(engine, variable_names)
            # template_path = paths["RA_AI_SUMMARY_TEMPLATE"]
            # log_dir = paths["RA_AI_SUMMARY_LOGPATH"]
            # output_dir = paths["RA_AI_SUMMARY_DATAPATH"]
        else:
            logging.error(f"Unknown pipeline type: {pipeline_type}")
            sys.exit(1)
    except Exception as e:
        logging.error("Failed to fetch output paths from Vision_Variables.")
        sys.exit(1)

    os.makedirs(log_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    setup_logging(log_dir=log_dir)

    # Logging has already been configured at import time using utils.logging_config.
    # Ensure the pipeline-specific log directory exists so further file handlers can rotate there if configured.
    logger.info("Pipeline type: %s", pipeline_type)
    logger.info("Template path: %s", template_path)
    logger.info("Log directory: %s", log_dir)
    logger.info("Output directory: %s", output_dir)
    logger.info("Pipeline started.")
    logger.info(f"DB config loaded, engine created")

    try:
        logger.info(f"Executing {pipeline_type} pipeline")
        
        if pipeline_type == "ra_summary":
            logger.info("Starting RA summary pipeline execution")
            run_ra_summary_pipeline(
                country=country,
                le_book=le_book,
                business_date=business_date,
                template_path=template_path,
                output_dir=output_dir,
                engine=engine,
                config=config
            )
        elif pipeline_type == "bs_summary":
            logger.info("Starting BS summary pipeline execution")
            run_bs_summary_pipeline(
                engine=engine,
                country=country,
                le_book=le_book,
                business_date=business_date,
                config=config,
                template_path=template_path,
                output_dir=output_dir
            )
        
        logger.info("Pipeline completed successfully")
        logger.info(f"Summary exported to {output_dir}")
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
