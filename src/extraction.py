import pandas as pd
import json
from utils.logging_config import logger

def extract_transactions(file_path: str) -> pd.DataFrame:
    """
    Extract the transactions data from a JSON file

    :param file_path: The path to the transactions JSON file.
    :type file_path: str
    :return: A pandas DataFrame containing the extracted transaction data.
    :rtype: pd.DataFrame
    :raises ValueError: If the extracted data is None or empty.
    :raises Exception: If there is an error during data extraction.
    """
    try:
        logger.info(f"Starting extraction of transactions from {file_path}...")
        with open(file_path, 'r') as file:
            transactions_data = json.load(file)

        transactions_df = pd.DataFrame(transactions_data)
        
        if transactions_df.empty:
            logger.error(f"No data found in {file_path}.")
            raise ValueError(f"No data found in {file_path}")
        
        logger.info(f"Successfully extracted {len(transactions_df)} transactions.")
        return transactions_df
    
    except Exception as e:
        logger.error(f"Error extracting transactions from {file_path}: {e}")
        raise

def extract_chargebacks(file_path: str) -> pd.DataFrame:
    """
    Extract the chargebacks data from a CSV file

    :param file_path: The path to the chargebacks CSV file.
    :type file_path: str
    :return: A pandas DataFrame containing the extracted chargeback data.
    :rtype: pd.DataFrame
    :raises ValueError: If the extracted data is None or empty.
    :raises Exception: If there is an error during data extraction.
    """
    try:
        logger.info(f"Starting extraction of chargebacks from {file_path}...")
        chargebacks_df = pd.read_csv(file_path)
        
        if chargebacks_df.empty:
            logger.error(f"No data found in {file_path}.")
            raise ValueError(f"No data found in {file_path}")
        
        logger.info(f"Successfully extracted {len(chargebacks_df)} chargebacks.")
        return chargebacks_df
    
    except Exception as e:
        logger.error(f"Error extracting chargebacks from {file_path}: {e}")
        raise

def extract_orders(file_path: str) -> pd.DataFrame:
    """
    Extract the orders data from a JSON file

    :param file_path: The path to the orders JSON file.
    :type file_path: str
    :return: A pandas DataFrame containing the extracted order data.
    :rtype: pd.DataFrame
    :raises ValueError: If the extracted data is None or empty.
    :raises Exception: If there is an error during data extraction.
    """
    
    try:
        logger.info(f"Starting extraction of orders from {file_path}...")
        with open(file_path, 'r') as file:
            orders_data = json.load(file)

        orders_df = pd.json_normalize(orders_data)
        
        if orders_df.empty:
            logger.error(f"No data found in {file_path}.")
            raise ValueError(f"No data found in {file_path}")
        
        logger.info(f"Successfully extracted {len(orders_df)} orders.")
        return orders_df
    
    except Exception as e:
        logger.error(f"Error extracting orders from {file_path}: {e}")
        raise
