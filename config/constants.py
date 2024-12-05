import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration parameters from environment variables
TRANSACTIONS_FILE_PATH = os.getenv('TRANSACTIONS_FILE_PATH', 'data/transactions.json')
ORDERS_FILE_PATH = os.getenv('ORDERS_FILE_PATH', 'data/orders.json')
CHARGEBACKS_FILE_PATH= os.getenv('CHARGEBACKS_FILE_PATH', 'data/chargebacks.csv') 

PRECISION_LIMIT = int(os.getenv('PRECISION_LIMIT', 2))