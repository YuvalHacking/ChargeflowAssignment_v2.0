# Chargeflow Assignment - Version 2.0

This data pipeline project is designed to process and transform financial data for an e-commerce platform as part of Chargeflow Assignment

## Table of Contents

- [Project Structure](#project-structure)
- [Requirements](#requirements)
- [Setup](#setup)
- [Usage](#usage)
- [Architecture](#architecture)

## Project Structure

```
ChargeflowAssignment_v2.0/
├── config/----------------------------------------- Configuration files
│   └── constants.py
├── data/------------------------------------------- Mock data of each datasources
│   ├── chargebacks.csv
│   ├── orders.json
│   └── transactions.json
├── scripts/---------------------------------------- Scripts for data processing
│   └── pipeline.py
├── src/
│   ├── transformation/----------------------------- Data transformations
│   │   ├── validations/---------------------------- Validations for each datasources
│   │   │   ├── chargeback.py
│   │   │   ├── orders.py
│   │   │   └── transactions.py
│   │   ├── analysis.py ----------------------------- Analysis of the data and outputs metrics
│   │   ├── clean.py--------------------------------- Cleans the data before usage
│   │   └── normalize.py----------------------------- Normalize data before usage
│   ├── extraction.py-------------------------------- Extract the data from each datasources
│   └── output.py ----------------------------------- Outputs the metrics result of the pipeline 
├── utils/------------------------------------------- Utility functions
│   └── logging_config.py
├── .env
├── architecture.png
├── launch.json
├── README.md
└── requirements.txt
```

## Requirements

- Python 3.9+

## Setup

1. **Clone the repository**:
    ```sh
    git clone https://github.com/YuvalHacking/ChargeflowAssignment_v2.0
    cd ChargeflowAssignment_v2.0
    ```

2. **Install the required Python packages**:
    ```sh
    pip install -r requirements.txt
    ```

## Usage

**Run the Data Pipeline**:
```sh
python -m scripts.pipeline
```

## Architecture
![architecture](https://github.com/user-attachments/assets/054d6858-eeeb-4f56-ab26-8992a5cf8bf6)
