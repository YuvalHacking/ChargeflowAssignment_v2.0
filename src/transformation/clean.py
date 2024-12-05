import pandas as pd
from utils.logging_config import logger

def clean_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data in the orders dataFrame

    :param orders: The orders dataFrame to clean.
    :type orders: pd.DataFrame
    :return: The cleaned orders dataFrame.
    :rtype: pd.DataFrame
    """
    logger.info(f"Starting chargebacks data cleaning")

    try:
        # Remove rows with missing values
        orders = orders.dropna()

        # Remove duplicates
        orders.loc[:, 'order_id'] = orders['order_id'].str.strip()
        orders = orders.drop_duplicates(subset=["order_id"])

        logger.info(f"Successfully cleaned {len(orders)} chargebacks")

        return orders

    except Exception as e:
        logger.error(f"Error cleaning orders data: {e}")
        raise

def clean_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data in the transactions dataFrame

    :param transactions: The transactions dataFrame to clean.
    :type transactions: pd.DataFrame
    :return: The cleaned transactions dataFrame.
    :rtype: pd.DataFrame
    """
    logger.info(f"Starting transactions data cleaning")

    try:
        # Remove rows with missing values
        transactions = transactions.dropna()

        # Remove duplicates
        transactions.loc[:, 'transaction_id'] = transactions['transaction_id'].str.strip()
        transactions = transactions.drop_duplicates(subset=["transaction_id"])
  
        logger.info(f"Successfully cleaned {len(transactions)} transactions")

        return transactions

    except Exception as e:
        logger.error(f"Error cleaning transactions data: {e}")
        raise

def clean_chargebacks(chargebacks: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the data in the chargebacks dataFrame

    :param chargebacks: The chargebacks dataFrame to clean.
    :type chargebacks: pd.DataFrame
    :return: The cleaned chargebacks dataFrame.
    :rtype: pd.DataFrame
    """
    logger.info(f"Starting transactions data cleaning")

    try:
        # Remove rows with missing values
        chargebacks = chargebacks.dropna()

        # Remove duplicates
        chargebacks.loc[:, 'transaction_id'] = chargebacks['transaction_id'].str.strip()
        chargebacks = chargebacks.drop_duplicates(subset=["transaction_id"])

        logger.info(f"Successfully cleaned {len(chargebacks)} chargebacks")

        return chargebacks

    except Exception as e:
        logger.error(f"Error cleaning chargebacks data: {e}")
        raise