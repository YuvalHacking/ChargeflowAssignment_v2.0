from pydantic import BaseModel, ValidationError, field_validator, model_validator, Field
from typing import Literal
import pandas as pd
from utils.logging_config import logger

class Chargeback(BaseModel):
    transaction_id: str = Field(min_length=36, max_length=36)
    dispute_date: str
    amount: float
    reason_code: str = Field(min_length=1, max_length=30)
    status: Literal['open', 'resolved']
    resolution_date: str

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

    @field_validator('dispute_date')
    def validate_dispute_date(cls, value: str) -> str:
        """
        Validate that the dispute date is in the correct date format.

        :param value: The dispute date to validate.
        :type value: str
        :return: The validated dispute date.
        :rtype: str
        :raises ValueError: If the dispute date format is invalid.
        """

        try:
            pd.to_datetime(value)
            return value

        except ValueError:
            logger.error(f"Invalid dispute date format: {value}")
            raise ValueError(f"Invalid dispute date format: {value}")

    @field_validator('resolution_date', mode='before')
    def validate_resolution_date(cls, value: str) -> str:
        """
        Validate that the resolution date is in the correct date format.

        :param value: The resolution date to validate.
        :type value: str
        :return: The validated resolution date.
        :rtype: str
        :raises ValueError: If the resolution date format is invalid.
        """

        try:
            pd.to_datetime(value)
            return value

        except ValueError:
            logger.error(f"Invalid resolution date format: {value}")
            raise ValueError(f"Invalid resolution date format: {value}")
        
    # Validate that dispute_date is earlier or equal to resolution_date
    @model_validator(mode="after")
    def dispute_before_resolution(cls, values: dict):
        """
        Validate that the dispute date is earlier than or equal to the resolution date.

        :param values: A dictionary containing the chargeback values
        :type values: dict
        :return: The original values if the validation passes.
        :rtype: dict
        :raises ValueError: If the dispute date is later than the resolution date.
        """

        dispute_date, resolution_date = values.dispute_date, values.resolution_date

        if dispute_date > resolution_date:
            logger.error(f"Dispute date: {dispute_date} must be earlier than or equal to resolution date: {resolution_date}")
            raise ValueError(f"Dispute date: {dispute_date} must be earlier than or equal to resolution date: {resolution_date}")
        
        return values

    
def validate_chargebacks(chargebacks: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the chargebacks data.

    :param chargebacks: DataFrame containing chargeback data.
    :type chargebacks: pd.DataFrame
    :return: DataFrame containing validated chargebacks.
    :rtype: pd.DataFrame
    :raises ValidationError: If any validation fails.
    """
    
    logger.info("Validating chargeback data")

    validated_chargebacks = []
    chargebacks_list = chargebacks.to_dict(orient='records')

    for chargeback in chargebacks_list:
        try:
            validated_chargeback = Chargeback(**chargeback)
            validated_chargebacks.append(validated_chargeback.model_dump())
            
        except ValidationError as e:
            logger.error(f"Validation error in chargebacks with transaction id "
             f"{chargeback.get('transaction_id')}: {e}")
            raise e

    logger.info(f"Validated {len(validated_chargebacks)} chargebacks successfully.")
    validated_chargebacks_df = pd.DataFrame(validated_chargebacks)
    
    return validated_chargebacks_df