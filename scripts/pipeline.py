
import time

from utils.logging_config import log_indent, logger
from config.constants import ORDERS_FILE_PATH, TRANSACTIONS_FILE_PATH, CHARGEBACKS_FILE_PATH

from src.transformation.analysis import calculate_business_metrics
from src.extraction import extract_transactions, extract_chargebacks, extract_orders
from src.transformation.clean import clean_orders, clean_transactions, clean_chargebacks
from src.output import print_analysis
from src.transformation.validations.orders import validate_orders
from src.transformation.validations.transactions import validate_transactions
from src.transformation.validations.orders import validate_orders
from src.transformation.validations.chargeback import validate_chargebacks
from src.transformation.normalize import (normalize_orders, normalize_transactions,
                                          normalize_chargebacks, match_dataframes)

def main():
    logger.info("Starting the data pipeline")
    start_time = time.time()

    try:
        # Step 1: Extract Data
        logger.info("Starting extraction of the data from the data sources")
        with log_indent():
            orders = extract_orders(ORDERS_FILE_PATH)
            transactions = extract_transactions(TRANSACTIONS_FILE_PATH)
            chargebacks = extract_chargebacks(CHARGEBACKS_FILE_PATH)
        logger.info("Finished extracting the data")

        # Step 2: Clean Data
        logger.info("Starting cleaning the data")
        with log_indent():
            orders = clean_orders(orders)
            transactions = clean_transactions(transactions)
            chargebacks = clean_chargebacks(chargebacks)
        logger.info("Finished cleaning the data")

        # Step 3: Validate Data
        logger.info("Starting data validation")
        with log_indent():
            orders = validate_orders(orders)
            transactions = validate_transactions(transactions, orders[["order_id", "total_amount"]])
            chargebacks = validate_chargebacks(chargebacks)
        logger.info("Finished data validation")

        # Step 4: Normalize Data
        logger.info("Starting data normalizing")
        with log_indent():
            orders = normalize_orders(orders)
            transactions = normalize_transactions(transactions)
            chargebacks = normalize_chargebacks(chargebacks) 
            merged = match_dataframes(orders, transactions, chargebacks)
        logger.info("Finished data normalizing")

        # Step 5: Get analysis metrics
        metrics = calculate_business_metrics(merged, transactions, chargebacks)

        # Step 6: Output for analysis
        print_analysis(metrics)

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(f"Data pipeline completed successfully in {elapsed_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in data pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
