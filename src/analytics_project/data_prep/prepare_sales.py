"""scripts/data_preparation/prepare_sales.py

This script reads data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys
import pandas as pd

# Ensure project root is in sys.path for local imports
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

# Import local modules (e.g. utils/logger.py)
from utils.logger import logger
from utils.data_scrubber import DataScrubber

# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent

# Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data


# Ensure the directories exist or create them
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate sales records.

    Uses TransactionID as the unique identifier when present, otherwise
    falls back to removing full-row duplicates.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial = len(df)
    if "TransactionID" in df.columns:
        df = df.drop_duplicates(subset=["TransactionID"], keep="first")
        logger.info("Removed duplicates based on TransactionID")
    else:
        df = df.drop_duplicates()
        logger.info("Removed full-row duplicates (TransactionID not found)")

    logger.info(f"Removed {initial - len(df)} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values using conservative business rules.

    - Drop rows missing TransactionID or SaleAmount (critical)
    - Drop rows missing CustomerID or ProductID
    - Fill missing DiscountPercent with 0
    - Fill missing PaymentType with 'Unknown'
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    # Log missing before
    missing_before = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_before}")

    # Drop rows missing critical identifiers or amounts
    critical = [
        c for c in ["TransactionID", "SaleAmount", "CustomerID", "ProductID"] if c in df.columns
    ]
    if critical:
        before = len(df)
        df = df.dropna(subset=critical)
        logger.info(f"Dropped {before - len(df)} rows missing critical columns: {critical}")

    # Fill DiscountPercent with 0 if missing
    if "DiscountPercent" in df.columns and df["DiscountPercent"].isna().sum() > 0:
        df["DiscountPercent"] = df["DiscountPercent"].fillna(0)
        logger.info("Filled missing DiscountPercent with 0")

    # Fill PaymentType with 'Unknown'
    if "PaymentType" in df.columns and df["PaymentType"].isna().sum() > 0:
        df["PaymentType"] = df["PaymentType"].fillna("Unknown")
        logger.info("Filled missing PaymentType with 'Unknown'")

    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers from numeric sale columns using IQR and rules.

    Applies IQR to SaleAmount and DiscountPercent (where present), and
    removes negative sale amounts.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial = len(df)

    # IQR on SaleAmount
    if "SaleAmount" in df.columns and pd.api.types.is_numeric_dtype(df["SaleAmount"]):
        q1 = df["SaleAmount"].quantile(0.25)
        q3 = df["SaleAmount"].quantile(0.75)
        iqr = q3 - q1
        if iqr and not pd.isna(iqr):
            lb = q1 - 1.5 * iqr
            ub = q3 + 1.5 * iqr
            before = len(df)
            df = df[(df["SaleAmount"] >= lb) & (df["SaleAmount"] <= ub)]
            logger.info(
                f"Applied IQR removal to SaleAmount bounds [{lb}, {ub}] removed {before - len(df)} rows"
            )

    # Remove negative SaleAmount explicitly
    if "SaleAmount" in df.columns and pd.api.types.is_numeric_dtype(df["SaleAmount"]):
        neg = df[df["SaleAmount"] < 0].shape[0]
        if neg > 0:
            df = df[df["SaleAmount"] >= 0]
            logger.info(f"Removed {neg} rows with negative SaleAmount")

    # DiscountPercent bounds 0..100
    if "DiscountPercent" in df.columns and pd.api.types.is_numeric_dtype(df["DiscountPercent"]):
        before = len(df)
        df = df[(df["DiscountPercent"] >= 0) & (df["DiscountPercent"] <= 100)]
        logger.info(f"Applied bounds to DiscountPercent removed {before - len(df)} rows")

    logger.info(f"Removed {initial - len(df)} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize formats for sale records.

    - Parse SaleDate to ISO (YYYY-MM-DD) where possible
    - Strip string fields
    - Drop empty unnamed columns
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    # Drop columns that are unnamed and empty
    unnamed = [c for c in df.columns if str(c).startswith("Unnamed")]
    for c in unnamed:
        # drop if entirely null
        if df[c].isna().all():
            df = df.drop(columns=[c])
            logger.info(f"Dropped empty column {c}")

    # Parse SaleDate
    if "SaleDate" in df.columns:
        try:
            df["SaleDate"] = pd.to_datetime(df["SaleDate"], errors="coerce").dt.strftime("%Y-%m-%d")
        except Exception:
            logger.debug("Could not parse SaleDate to datetime; leaving as-is")

    # Strip string columns such as PaymentType
    for col in ["PaymentType", "StoreID", "CampaignID"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate sale data against simple business rules.

    - Ensure TransactionID positive if present
    - Ensure SaleAmount numeric and non-negative
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")

    if "TransactionID" in df.columns:
        bad = df[df["TransactionID"] <= 0].shape[0]
        if bad > 0:
            df = df[df["TransactionID"] > 0]
            logger.info(f"Dropped {bad} rows with non-positive TransactionID")

    if "SaleAmount" in df.columns and pd.api.types.is_numeric_dtype(df["SaleAmount"]):
        bad = df[df["SaleAmount"].isna()].shape[0]
        if bad > 0:
            df = df.dropna(subset=["SaleAmount"])
            logger.info(f"Dropped {bad} rows with non-numeric SaleAmount")

    logger.info("Data validation complete")
    return df


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """Process sales data for analytics."""
    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"
    output_file = "sales_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}"
        for old, new in zip(original_columns, df.columns, strict=True)
        if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Validate data
    df = validate_data(df)

    # Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()
