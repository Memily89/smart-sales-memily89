"""scripts/data_preparation/prepare_products.py

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

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 3 parents are needed)
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent.parent))

# Import local modules (e.g. utils/logger.py)
# Optional: Use a data_scrubber module for common data cleaning tasks
from analytics_project.utils.logger import logger

# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR  # This is the analytics_project directory
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data


# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
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

    # Add data profiling to understand the dataset
    logger.info(f"Column datatypes:\n{df.dtypes}")
    logger.info(f"Number of unique values:\n{df.nunique()}")

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


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)

    # For products, ProductID is the unique identifier
    # Remove duplicates based on ProductID (keep first occurrence)
    if "productid" in df.columns:
        logger.info("Removing duplicates based on productid column")
        df = df.drop_duplicates(subset=["productid"], keep="first")
        logger.info("Duplicates removed based on ProductID")
    else:
        # Fallback: remove all duplicate rows
        logger.warning("productid column not found, removing complete duplicate rows instead")
        df = df.drop_duplicates()

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values by filling or dropping.

    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values by column before handling
    # NA means missing or "not a number" - ask your AI for details
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")

    # TODO: OPTIONAL - We can implement appropriate missing value handling
    # specific to our data.
    # For example: Different strategies may be needed for different columns
    # USE YOUR COLUMN NAMES - these are just examples
    # df['product_name'].fillna('Unknown Product', inplace=True)
    # df['description'].fillna('', inplace=True)
    # df['price'].fillna(df['price'].median(), inplace=True)
    # df['category'].fillna(df['category'].mode()[0], inplace=True)
    # df.dropna(subset=['product_code'], inplace=True)  # Remove rows without product code

    # Log missing values by column after handling
    missing_after = df.isna().sum()
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers based on thresholds.

    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    # Use IQR-based outlier removal for numeric product fields
    numeric_candidates = ["unitprice", "stockcount"]
    for col in numeric_candidates:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            if pd.isna(iqr) or iqr == 0:
                logger.debug(f"Skipping IQR outlier removal for {col}: IQR={iqr}")
                continue
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            before = len(df)
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            after = len(df)
            logger.info(
                f"Applied IQR outlier removal to {col}: bounds [{lower_bound}, {upper_bound}] - removed {before - after} rows"
            )

    # Additional simple sanity rules (no negative prices/stock)
    if "unitprice" in df.columns and pd.api.types.is_numeric_dtype(df["unitprice"]):
        neg_prices = df[df["unitprice"] < 0].shape[0]
        if neg_prices > 0:
            logger.info(f"Removing {neg_prices} rows with negative unitprice")
            df = df[df["unitprice"] >= 0]

    if "stockcount" in df.columns and pd.api.types.is_numeric_dtype(df["stockcount"]):
        neg_stock = df[df["stockcount"] < 0].shape[0]
        if neg_stock > 0:
            logger.info(f"Removing {neg_stock} rows with negative stockcount")
            df = df[df["stockcount"] >= 0]

    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the formatting of various columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with standardized formatting.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    # Standardize textual fields
    if "productname" in df.columns:
        # Strip whitespace and title-case common product names
        df["productname"] = df["productname"].astype(str).str.strip().str.title()

    if "category" in df.columns:
        df["category"] = df["category"].astype(str).str.strip().str.lower()

    if "supplier" in df.columns:
        df["supplier"] = df["supplier"].astype(str).str.strip()

    # Round currency/float fields
    if "unitprice" in df.columns and pd.api.types.is_numeric_dtype(df["unitprice"]):
        df["unitprice"] = df["unitprice"].round(2)

    # Ensure integer-like fields are integers
    if "stockcount" in df.columns and pd.api.types.is_numeric_dtype(df["stockcount"]):
        df["stockcount"] = df["stockcount"].astype(int)

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data against business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Validated DataFrame.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")

    # ProductID must be positive
    if "productid" in df.columns:
        invalid_ids = df[df["productid"] <= 0].shape[0]
        if invalid_ids > 0:
            logger.info(f"Dropping {invalid_ids} rows with non-positive productid")
            df = df[df["productid"] > 0]

    # UnitPrice must be non-negative
    if "unitprice" in df.columns and pd.api.types.is_numeric_dtype(df["unitprice"]):
        neg_prices = df[df["unitprice"] < 0].shape[0]
        if neg_prices > 0:
            logger.info(f"Dropping {neg_prices} rows with negative unitprice")
            df = df[df["unitprice"] >= 0]

    # StockCount should be integer >= 0
    if "stockcount" in df.columns and pd.api.types.is_numeric_dtype(df["stockcount"]):
        neg_stock = df[df["stockcount"] < 0].shape[0]
        if neg_stock > 0:
            logger.info(f"Dropping {neg_stock} rows with negative stockcount")
            df = df[df["stockcount"] >= 0]

    logger.info("Data validation complete")
    return df


def main() -> None:
    """Process product data for analytics."""
    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

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

    # TODO:Remove outliers
    df = remove_outliers(df)

    # TODO: Validate data
    df = validate_data(df)

    # TODO: Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")


# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()
