from tabulate import tabulate
from utils.logging_config import logger

def print_analysis(metrics: dict) -> None:
    """
    Print the analysis of various transaction metrics.


    :param metrics: A dictionary containing various transaction metrics.
        - 'daily_transactions': DataFrame with daily transaction metrics.
        - 'chargeback_rate': DataFrame with chargeback rate by payment method.
        - 'failed_transaction_analysis': DataFrame with failed transaction analysis.
        - 'payment_method_performance': DataFrame with payment method performance.
        - 'payment_success_rate': Float representing the payment success rate.
    :type metrics: dict
    :return: None
    :rtype: None
    """

    logger.info(f"Starting printing the pipeline analysis")

    try:
        print("Daily Transaction Metrics:")
        print(tabulate(metrics['daily_transactions'], headers='keys', tablefmt='grid', showindex=False))

        print("\nChargeback Rate by Payment Method:")
        print(tabulate(metrics['chargeback_rate'], headers='keys', tablefmt='grid', showindex=False))

        print("\nFailed Transaction Analysis:")
        print(tabulate(metrics['failed_transaction_analysis'], headers='keys', tablefmt='grid', showindex=False))

        print("\nPayment method performance:")
        print(tabulate(metrics['payment_method_performance'], headers='keys', tablefmt='grid', showindex=False))

        print(f"\nPayment Success Rate: {metrics['payment_success_rate']}")

        logger.info(f"Successfully printed the pipeline analysis")

    except Exception as e:
        logger.error(f"Error printing the pipeline analysis: {e}")
        raise