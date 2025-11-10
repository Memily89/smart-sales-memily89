"""Data scrubber module for cleaning and preparing data.

This module provides utilities for common data cleaning tasks such as removing
duplicates, handling missing values, and removing outliers.
"""

import pandas as pd


class DataScrubber:
    """A class to handle common data cleaning operations."""

    def __init__(self, df: pd.DataFrame):
        """Initialize the DataScrubber with a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to scrub.
        """
        self.df = df.copy()

    def remove_duplicate_records(self) -> pd.DataFrame:
        """Remove duplicate rows from the DataFrame.

        Returns:
            pd.DataFrame: DataFrame with duplicates removed.
        """
        return self.df.drop_duplicates()

    def handle_missing_values(
        self, strategy: str = "drop", fill_value: str | int | float | None = None
    ) -> pd.DataFrame:
        """Handle missing values in the DataFrame.

        Args:
            strategy (str): Strategy to use ('drop' or 'fill').
            fill_value: Value to fill missing values with if strategy is 'fill'.

        Returns:
            pd.DataFrame: DataFrame with missing values handled.
        """
        if strategy == "drop":
            return self.df.dropna()
        if strategy == "fill":
            return self.df.fillna(fill_value)
        raise ValueError(f"Unknown strategy: {strategy}")

    def remove_outliers(self, columns: list[str] | None = None) -> pd.DataFrame:
        """Remove outliers from specified columns using IQR method.

        Args:
            columns (list[str]): Columns to check for outliers. If None, check all numeric columns.

        Returns:
            pd.DataFrame: DataFrame with outliers removed.
        """
        df_cleaned = self.df.copy()
        cols = columns or df_cleaned.select_dtypes(include=["number"]).columns.tolist()

        for col in cols:
            if col in df_cleaned.columns:
                q1 = df_cleaned[col].quantile(0.25)
                q3 = df_cleaned[col].quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                df_cleaned = df_cleaned[
                    (df_cleaned[col] >= lower_bound) & (df_cleaned[col] <= upper_bound)
                ]

        return df_cleaned
