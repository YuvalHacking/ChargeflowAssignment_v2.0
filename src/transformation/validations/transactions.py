from pydantic import BaseModel, ValidationError, field_validator, Field
from typing import Literal, Optional
import pandas as pd
from utils.logging_config import logger

class PaymentMethod(BaseModel):
    type: Literal['credit_card', 'debit_card', 'wallet']  
    provider: str = Field(min_length=2, max_length=40)  

class Transaction(BaseModel):
    transaction_id: str = Field(min_length=36, max_length=36)
    order_id: str = Field(min_length=6, max_length=30)
    timestamp: str
    amount: float
    currency: Literal['USD', 'EUR', 'GBP', 'INR', 'AUD', 'CAD']
    status: Literal['completed', 'failed', 'pending']  
    payment_method: PaymentMethod
    error_code: Optional[str] = Field(min_length=1, max_length=20) 


    @field_validator('timestamp')
    def validate_timestamp(cls, value: str):
        """
        Validate that the timestamp is in the correct date format.

        :param value: The timestamp to validate.
        :type value: str
        :return: The validated timestamp.
        :rtype: str
        :raises ValueError: If the timestamp format is invalid.
        """

        try:
            pd.to_datetime(value)
        except ValueError:
            logger.error(f"Invalid timestamp format: {value}")
            raise ValueError(f"Invalid timestamp format: {value}")
        return value
    
    @field_validator('amount')
    def validate_amount_positive(cls, value: float) -> float:
        """
        Validate the amount is a positive number.

        :param value: The amount to validate.
        :type value: float
        :return: The validated amount.
        :rtype: float
        :raises ValueError: If the amount is 0 or negative.
        """

        if value <= 0:
            logger.error("Invalid amount - must be greater than 0")
            raise ValueError("Invalid amount - must be greater than 0")
        
        return value
    
def validate_transactions(transactions: pd.DataFrame, orders_amount: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the transactions data.

    :param transactions: DataFrame containing transactions to validate.
    :type transactions: pd.DataFrame
    :param orders_amount: DataFrame the orders and the total amounts.
    :type orders_amount: pd.DataFrame
    :return: DataFrame with validated transactions.
    :rtype: pd.DataFrame
    :raises ValidationError: If any validation fails.
    """

    logger.info("Validating transactions data")

    transactions = validate_amounts_match(transactions, orders_amount)

    transactions_list = transactions.to_dict(orient='records')
    validated_transactions = []
    
    for transaction in transactions_list:
        try:
            validated_transaction = Transaction(**transaction)
            validated_transactions.append(validated_transaction.model_dump())
            
        except ValidationError as e:
            logger.error(f"Validation error in transaction {transaction['transaction_id']}: {e}")
            raise e  
        
    logger.info(f"Validated {len(validated_transactions)} transactions successfully.")
    validated_transactions_df = pd.DataFrame(validated_transactions)

    return validated_transactions_df

def validate_amounts_match(transactions: pd.DataFrame, orders_amount: pd.DataFrame) -> pd.DataFrame:
    """
    Validate that the amounts in transactions match the total amounts in orders.

    :param transactions: DataFrame containing transactions to validate.
    :type transactions: pd.DataFrame
    :param orders_amount: DataFrame the orders and the total amounts.
    :type orders_amount: pd.DataFrame
    :return: DataFrame with validated transactions.
    :rtype: pd.DataFrame
    :raises ValidationError: If any transactions amount dont fit their order total amount.
    """

    merged_amount = pd.merge(transactions, orders_amount, left_on="order_id", right_on="order_id", how="left")

    validated_transactions_amounts = merged_amount[merged_amount['amount'] == merged_amount['total_amount']]

    invalidated_transactions_amounts = len(transactions) - len(validated_transactions_amounts)

    if invalidated_transactions_amounts > 0:
        logger.error(f"{invalidated_transactions_amounts} transactions amount fields do not match their order total amount")
        raise ValueError(f"{invalidated_transactions_amounts} transactions amount fields do not match their order total amount")
    
    validated_transactions_amounts.drop('total_amount', axis=1, inplace=True)

    return validated_transactions_amounts
