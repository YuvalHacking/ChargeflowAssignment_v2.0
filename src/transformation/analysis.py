import pandas as pd
import textwrap
from utils.logging_config import log_indent, logger
from config.constants import PRECISION_LIMIT

precision_limit = PRECISION_LIMIT

def calculate_payment_success_rate(transactions: pd.DataFrame) -> float:
    """
    Calculate the payment success rate by dividing the number of completed transactions
    by the total number of transactions.

    :param transactions: The DataFrame containing transaction data.
    :type transactions: pd.DataFrame
    :return: The payment success rate.
    :rtype: float
    """

    logger.info(f"Starting calculating payment success rate")

    try:
        success_count = len(transactions[transactions['status'] == 'completed'])
        total_count = len(transactions)

        success_rate = (success_count * 100 / total_count) if total_count > 0 else 0.0
        
        logger.info(f"Successfully calculated payment success rate")

        return f"{success_rate:.2f}%"
    
    except Exception as e:
        logger.error(f"Error calculating the payment success rate: {e}")
        raise

def calculate_daily_metrics(transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily metrics including transaction volume and value.

    :param transactions: The DataFrame containing transaction data.
    :type transactions: pd.DataFrame
    :return: DataFrame with daily metrics.
    :rtype: pd.DataFrame
    """

    logger.info(f"Starting calculating the daily metrics")

    try:
        completed_transactions = transactions[transactions["status"] == "completed"]

        daily_transactions = completed_transactions.groupby(completed_transactions['timestamp'].dt.strftime('%d-%m-%Y')).agg(
            transaction_volume=("transaction_id", "count"),  
            transaction_value=("amount", "sum")
        ).reset_index()

        daily_transactions = daily_transactions.rename(columns={'timestamp': 'day', 
                                                                'transaction_volume': 'volume',
                                                                'transaction_value': 'value'})
        daily_transactions = daily_transactions.sort_values(by='day')

        logger.info(f"Successfully calculated the daily metrics")

        return daily_transactions
    
    except Exception as e:
        logger.error(f"Error calculating the daily metrics: {e}")
        raise

def calculate_chargeback_rates(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate chargeback rates by payment method types.

    :param merged: The DataFrame containing merged transaction and chargeback data.
    :type merged: pd.DataFrame
    :return: DataFrame with chargeback rates.
    :rtype: pd.DataFrame
    """

    logger.info(f"Starting calculating the chargeback rates")

    try:
        # Add a column to indicate if a transaction is a chargeback
        merged["is_chargeback"] = merged["chargeback_dispute_date"].notnull()

        # Group by payment method and calculate metrics
        chargeback_stats = merged.groupby("transaction_payment_method.type").agg(
            total_transactions=("transaction_transaction_id", "count"),
            total_chargebacks=("is_chargeback", "sum")
        ).reset_index()

        # Add chargeback rate column
        chargeback_stats["chargeback_rate"] = ((
            chargeback_stats["total_chargebacks"] / chargeback_stats["total_transactions"]
        ) * 100).round(precision_limit)
    
        logger.info(f"Successfully calculated the chargeback rates")

        return chargeback_stats
    
    except Exception as e:
        logger.error(f"Error calculating the chargeback rates: {e}")
        raise

def analyze_failed_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze failed transactions and return relevant metrics.

    :param transactions: The DataFrame containing transaction data.
    :type transactions: pd.DataFrame
    :return: DataFrame with failed transaction analysis.
    :rtype: pd.DataFrame
    """

    logger.info(f"Starting analyzing the failed transactions")

    try:
        failed_transactions = transactions[transactions['status'] == 'failed']

        # Group by payment method and currency and calculate the count of failed transactions and sum of amounts
        failed_transactions_grouped = failed_transactions.groupby(['payment_method.type', 'currency']).agg(
                                    transaction_count=('transaction_id', 'count'),
                                    value=('amount', 'sum')).reset_index()
                                
        failed_transactions_grouped['value'] = failed_transactions_grouped['value'].round(precision_limit)
        
        # Create an amounts column for each payment method type that contains all currency amounts separated
        failed_currency_amounts = failed_transactions_grouped.groupby(['payment_method.type']).apply(
            lambda group:  group[['value', 'currency']].to_dict(orient='records')).reset_index(name='amounts')

        # Sum the failed transaction counts for each payment method type
        failed_transactions_counts = failed_transactions_grouped.groupby('payment_method.type').agg(
                                    transaction_count=('transaction_count', 'sum')).reset_index()

        final_result = pd.merge(failed_currency_amounts, failed_transactions_counts, on='payment_method.type', how="left")
        final_result.rename(columns={'transaction_count': 'failed_transaction_count'}, inplace=True)

        # Format the amounts column as a string and wrap text to a fixed width for printing
        final_result['amounts'] = final_result['amounts'].apply(lambda x: str(x))
        final_result['amounts'] = final_result['amounts'].apply(lambda x: "\n".join(textwrap.wrap(x, width=40)))

        # final_result = failed_transactions.groupby('payment_method.type').agg(
        #     failed_transaction_count=('transaction_id', 'count'),
        #     failed_transaction_value=('amount', 'sum')
        # ).reset_index()

        logger.info(f"Successfully analyzed the failed transactions")

        return final_result

    except Exception as e:
        logger.error(f"Error analyzing the failed transactions: {e}")
        raise

def calculate_payment_method_performance(transactions: pd.DataFrame, chargebacks: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate performance metrics for each payment method.

    :param transactions: The DataFrame containing transaction data.
    :type transactions: pd.DataFrame
    :param chargebacks: The DataFrame containing chargeback data.
    :type chargebacks: pd.DataFrame
    :return: DataFrame with payment method performance metrics.
    :rtype: pd.DataFrame
    """

    logger.info(f"Starting calculating the payment method performance")

    try:
        # Add disputed indiction column
        transactions['disputed'] = transactions['transaction_id'].isin(chargebacks['transaction_id'])

        grouped_payment_methods = transactions.groupby(['payment_method.type'])

        # Calculate metrics
        performance = grouped_payment_methods.agg(
            total_transactions=('transaction_id', 'count'),
            completed_transactions=('status', lambda status: (status == 'completed').sum()),
            failed_transactions=('status', lambda status: (status == 'failed').sum()),
            disputed_transactions=('disputed', 'sum'),
            total_amount=('amount', 'sum'),
            average_amount=('amount', 'mean')
        )

        # Add derived metrics
        performance['success_rate'] = (performance['completed_transactions'] / performance['total_transactions']).round(precision_limit) * 100
        performance['failure_rate'] = (performance['failed_transactions'] / performance['total_transactions']).round(precision_limit) * 100
        performance['dispute_rate'] = (performance['disputed_transactions'] / performance['total_transactions']).round(precision_limit) * 100

        performance = performance.reset_index()

        logger.info(f"Successfully calculated the payment method performance")

        return performance

    except Exception as e:
        logger.error(f"Error calculating the payment method performance: {e}")
        raise
    
def calculate_business_metrics(merged: pd.DataFrame, transactions: pd.DataFrame, chargebacks: pd.DataFrame) -> dict:
    """
    Calculate key business metrics including daily transactions, chargeback rates, failed transaction analysis,
    payment method performance, and payment success rate.

    :param merged: The DataFrame containing merged transaction and chargeback data.
    :type merged: pd.DataFrame
    :param transactions: The DataFrame containing transaction data.
    :type transactions: pd.DataFrame
    :param chargebacks: The DataFrame containing chargeback data.
    :type chargebacks: pd.DataFrame
    :return: Dictionary with key business metrics.
    :rtype: dict
    """
    
    logger.info(f"Starting calculating the business metrics")

    try:
        with log_indent():
            metrics = {
                "daily_transactions": calculate_daily_metrics(transactions),
                "chargeback_rate": calculate_chargeback_rates(merged),
                "failed_transaction_analysis": analyze_failed_transactions(transactions),
                "payment_method_performance": calculate_payment_method_performance(transactions, chargebacks),
                "payment_success_rate": calculate_payment_success_rate(transactions)
            }

        logger.info(f"Successfully calculated the business metrics")

        return metrics
        
    except Exception as e:
        logger.error(f"Error calculating the business metrics: {e}")
        raise