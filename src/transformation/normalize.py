import pandas as pd
from utils.logging_config import logger

def normalize_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the orders data by converting certain columns to the appropriate data types.
    :param orders: The DataFrame containing orders data.
    :type orders: pd.DataFrame
    :return: The transformed DataFrame with normalized orders data.
    :rtype: pd.DataFrame
    """
    
    logger.info("Normalizing orders data")

    try:
        # Format the date column
        orders['timestamp'] = pd.to_datetime(orders['timestamp'])

        logger.info(f"Successfully normalized orders data")

        return orders

    except Exception as e:
        logger.error(f"Error normalizing orders data: {e}")
        raise

def normalize_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the transactions data by converting certain columns to the appropriate data types.

    :param transactions: The DataFrame containing transactions data.
    :type transactions: pd.DataFrame
    :return: The transformed DataFrame with normalized transactions data.
    :rtype: pd.DataFrame
    """
    
    logger.info(f"Starting normalizing transactions data")

    try:
        # Format the date column
        transactions['timestamp'] = pd.to_datetime(transactions['timestamp'])

        # Normalize payment_method column
        transactions = pd.json_normalize(transactions.to_dict(orient='records'))

        logger.info(f"Successfully normalized transactions data")

        return transactions

    except Exception as e:
        logger.error(f"Error normalizing transactions data: {e}")
        raise

def normalize_chargebacks(chargebacks: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the chargebacks data by converting certain columns to the appropriate data types.

    :param chargebacks: The DataFrame containing chargebacks data.
    :type chargebacks: pd.DataFrame
    :return: The transformed DataFrame with normalized chargebacks data.
    :rtype: pd.DataFrame
    """
    
    logger.info(f"Starting normalizing chargebacks data")

    try:
        # Format the dates column
        chargebacks[['dispute_date', 'resolution_date']] = chargebacks[['dispute_date', 'resolution_date']].apply(pd.to_datetime)

        logger.info(f"Successfully normalized chargebacks data")

        return chargebacks

    except Exception as e:
        logger.error(f"Error normalizing chargebacks data: {e}")
        raise

# Normalize and match chargebacks with transactions
def match_dataframes(orders: pd.DataFrame, transactions: pd.DataFrame, chargebacks: pd.DataFrame) -> pd.DataFrame:
    """
    Matching the chargebacks, transactions and orders and get the returned merged DataFrame.

    :param orders: The DataFrame containing orders data.
    :type orders: pd.DataFrame
    :param transactions: The DataFrame containing transactions data.
    :type transactions: pd.DataFrame
    :param chargebacks_df: The DataFrame containing chargebacks data.
    :type chargebacks_df: pd.DataFrame
    :return: A DataFrame containing merged transaction and chargeback data.
    :rtype: pd.DataFrame
    """

    logger.info("Matching the datasources data")

    try:
        # Rename columns to include the original DataFrame name as a prefix
        transactions_df = transactions.add_prefix('transaction_')
        chargebacks_df = chargebacks.add_prefix('chargeback_')
        orders_df = orders.add_prefix('order_')

        # Merge the dataframes
        merged_df = pd.merge(transactions_df, chargebacks_df, left_on="transaction_transaction_id", right_on="chargeback_transaction_id", how="left")
        merged_df = pd.merge(merged_df, orders_df, left_on="transaction_order_id", right_on="order_order_id", how="left")
        
        logger.info(f"Successfully matched the datasources data")

        return merged_df

    except Exception as e:
        logger.error(f"Error matching the datasources data: {e}")
        raise
