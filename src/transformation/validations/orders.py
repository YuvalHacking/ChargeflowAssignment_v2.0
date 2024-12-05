from pydantic import BaseModel, ValidationError, field_validator, model_validator, Field
from typing import Literal, List
import pandas as pd
from utils.logging_config import logger

class Item(BaseModel):
    product_id: str = Field(min_length=6, max_length=30)
    quantity: int
    unit_price: float

class Order(BaseModel):
    order_id: str = Field(min_length=6, max_length=30)
    customer_id: str = Field(min_length=36, max_length=36)
    timestamp: str
    total_amount: float
    currency: Literal['USD', 'EUR', 'GBP', 'INR', 'AUD', 'CAD']
    items: List[Item]
    payment_status: Literal['paid', 'failed', 'refunded']

    @field_validator('timestamp')
    def validate_timestamp(cls, value: str) -> str:
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
    
    @field_validator('total_amount')
    def validate_amount_positive(cls, value: float) -> float:
        """
        Validate the total amount is a positive number.

        :param value: The total amount to validate.
        :type value: float
        :return: The validated total amount.
        :rtype: float
        :raises ValueError: If the total amount is 0 or negative.
        """

        if value <= 0:
            logger.error("Invalid total amount - must be greater than 0")
            raise ValueError("Invalid total amount - must be greater than 0")
        
        return value
    
    @field_validator('items')
    def validate_items(cls, items: List[Item]) -> List[Item]:
        """
        Validate each item quantity and unit price are a positive number.

        :param value: The items to validate.
        :type value: List[Item]
        :return: The validated items.
        :rtype: List[Item]
        :raises ValueError: If an item quantity or unit price are 0 or negative.
        """

        for item in items:
            if item.quantity <= 0:
                logger.error("Invalid quantity - must be greater than 0")
                raise ValueError("Invalid quantity - must be greater than 0")

            if item.unit_price <= 0:
                logger.error("Invalid unit price - must be greater than 0")
                raise ValueError("Invalid unit price - must be greater than 0")

        return items
    
    @model_validator(mode="after")
    def dispute_before_resolution(cls, values: dict):
        """
        Validate that the total amount matches the sum of item amounts.

        :param values: order values to validate.
        :type values: dict
        :return: The validated values.
        :rtype: dict
        :raises ValueError: If the total amount does not match the sum of item amounts.
        """

        items, total_amount = values.items, values.total_amount

        total_items_amount = round(sum(item.quantity * item.unit_price for item in items), 6)

        if total_items_amount != total_amount:
            logger.error(f"Total amount: {total_amount} does not match sum of item amounts: {total_items_amount}")
            raise ValueError(f"Total amount: {total_amount} does not match sum of item amounts: {total_items_amount}")

        return values

def validate_orders(orders: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the orders data.

    :param orders_df: The DataFrame containing orders to validate.
    :type orders_df: pd.DataFrame
    :return: The validated orders dataFrame.
    :rtype: pd.DataFrame
    """

    logger.info("Validating orders data")
    
    validated_orders = []
    orders_list = orders.to_dict(orient='records')

    for order in orders_list:
        try:
            validated_order = Order(**order)
            validated_orders.append(validated_order.model_dump())
            
        except ValidationError as e:
            logger.error(f"Validation error in order {order.get('order_id')}: {e}")
            raise e

    logger.info(f"Validated {len(validated_orders)} orders successfully.")
    validated_orders_df = pd.DataFrame(validated_orders)
    
    return validated_orders_df