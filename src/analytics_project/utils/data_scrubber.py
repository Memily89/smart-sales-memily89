"""
utils/data_scrubber.py

Reusable utility class for performing common data cleaning and
preparation tasks on a pandas DataFrame.

This class provides methods for:
- Checking data consistency
- Removing duplicates
- Handling missing values
- Filtering outliers
- Renaming and reordering columns
- Formatting strings
- Parsing date fields

Use this class to perform similar cleaning operations across multiple files.
"""

import io
import pandas as pd
from typing import Dict, Tuple, Union, List


class DataScrubber:
    """A class to perform common data cleaning operations on a pandas DataFrame."""

    def __init__(self, df: pd.DataFrame):
        """Initialize the DataScrubber with a DataFrame."""
        self.df = df.copy()

    # ---------------------------
    # Consistency Checks
    # ---------------------------
    def check_data_consistency_before_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        """Return counts of null and duplicate entries before cleaning."""
        return {
            'null_counts': self.df.isnull().sum(),
            'duplicate_count': self.df.duplicated().sum(),
        }

    def check_data_consistency_after_cleaning(self) -> Dict[str, Union[pd.Series, int]]:
        """Return counts of null and duplicate entries after cleaning; assert all clean."""
        null_counts = self.df.isnull().sum()
        duplicate_count = self.df.duplicated().sum()
        assert null_counts.sum() == 0, "Data still contains null values after cleaning."
        assert duplicate_count == 0, "Data still contains duplicate records after cleaning."
        return {'null_counts': null_counts, 'duplicate_count': duplicate_count}

    # ---------------------------
    # Column Operations
    # ---------------------------
    def convert_column_to_new_data_type(self, column: str, new_type: type) -> pd.DataFrame:
        """Convert a column to a new data type."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].astype(new_type)
        return self.df

    def drop_columns(self, columns: List[str]) -> pd.DataFrame:
        """Drop specified columns from the DataFrame."""
        missing = [col for col in columns if col not in self.df.columns]
        if missing:
            raise ValueError(f"Columns not found in the DataFrame: {missing}")
        self.df = self.df.drop(columns=columns)
        return self.df

    def rename_columns(self, column_mapping: Dict[str, str]) -> pd.DataFrame:
        """Rename columns based on a mapping {old_name: new_name}."""
        missing = [col for col in column_mapping if col not in self.df.columns]
        if missing:
            raise ValueError(f"Columns not found in the DataFrame: {missing}")
        self.df = self.df.rename(columns=column_mapping)
        return self.df

    def reorder_columns(self, columns: List[str]) -> pd.DataFrame:
        """Reorder columns according to a specified list."""
        missing = [col for col in columns if col not in self.df.columns]
        if missing:
            raise ValueError(f"Columns not found in the DataFrame: {missing}")
        self.df = self.df[columns]
        return self.df

    # ---------------------------
    # String Formatting
    # ---------------------------
    def format_column_strings_to_lower_and_trim(self, column: str) -> pd.DataFrame:
        """Convert strings in a column to lowercase and trim whitespace."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].str.lower().str.strip()
        return self.df

    def format_column_strings_to_upper_and_trim(self, column: str) -> pd.DataFrame:
        """Convert strings in a column to uppercase and trim whitespace."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        self.df[column] = self.df[column].str.upper().str.strip()
        return self.df

    # ---------------------------
    # Missing Data
    # ---------------------------
    def handle_missing_data(
        self, drop: bool = False, fill_value: Union[None, float, int, str] = None
    ) -> pd.DataFrame:
        """Drop or fill missing data."""
        if drop:
            self.df = self.df.dropna()
        elif fill_value is not None:
            self.df = self.df.fillna(fill_value)
        return self.df

    # ---------------------------
    # Outliers
    # ---------------------------
    def filter_column_outliers(
        self, column: str, lower_bound: Union[float, int], upper_bound: Union[float, int]
    ) -> pd.DataFrame:
        """Filter outliers in a numeric column based on lower and upper bounds."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        self.df = self.df[(self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)]
        return self.df

    # ---------------------------
    # Date Parsing
    # ---------------------------
    def parse_dates_to_add_standard_datetime(self, column: str) -> pd.DataFrame:
        """Parse a column as datetime and add a new column 'StandardDateTime'."""
        if column not in self.df.columns:
            raise ValueError(f"Column '{column}' not found in the DataFrame.")
        self.df['StandardDateTime'] = pd.to_datetime(self.df[column], errors='coerce')
        return self.df

    # ---------------------------
    # Duplicates
    # ---------------------------
    def remove_duplicate_records(self) -> pd.DataFrame:
        """Remove duplicate rows."""
        self.df = self.df.drop_duplicates()
        return self.df

    # ---------------------------
    # Data Inspection
    # ---------------------------
    def inspect_data(self) -> Tuple[str, str]:
        """Return string representations of DataFrame info and summary statistics."""
        buffer = io.StringIO()
        self.df.info(buf=buffer)
        info_str = buffer.getvalue()
        describe_str = self.df.describe(include='all').to_string()
        return info_str, describe_str
