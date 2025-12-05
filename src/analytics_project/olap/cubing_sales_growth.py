"""OLAP cubing module - canonical implementation.

Builds a multidimensional OLAP cube aggregated by product, region and quarter.
Writes results to `data/olap_cubing_outputs/multidimensional_olap_cube.csv`.

This file is the repository's canonical OLAP cubing implementation and is a
clean, defensive implementation that mirrors the verified `cubing_campaign_fixed.py`.
"""

import pathlib
import sqlite3

import numpy as np
import pandas as pd

from analytics_project.utils.logger import logger

# Paths
THIS_DIR: pathlib.Path = pathlib.Path(__file__).resolve().parent
PACKAGE_DIR: pathlib.Path = THIS_DIR.parent
PROJECT_ROOT_DIR: pathlib.Path = PACKAGE_DIR.parent.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT_DIR / "data"
WAREHOUSE_DIR: pathlib.Path = DATA_DIR / "warehouse"
DB_PATH: pathlib.Path = WAREHOUSE_DIR / "smart_sales.db"

OLAP_OUTPUT_DIR: pathlib.Path = DATA_DIR / "olap_cubing_outputs"
OLAP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR / "multidimensional_olap_cube.csv"


def _find_table(conn: sqlite3.Connection, candidates: list[str]) -> str | None:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0].lower() for r in cur.fetchall()}
    for c in candidates:
        if c and c.lower() in tables:
            return c
    return None


def ingest_warehouse() -> pd.DataFrame:
    """Load and merge sales, product, and customer data from the warehouse database.

    Connects to the warehouse SQLite database, reads sales, product, and customer
    tables if available, and merges them on common keys (product_id, customer_id).

    Returns
    -------
    pd.DataFrame
        Merged dataframe containing sales data with product and customer information.

    Raises
    ------
    RuntimeError
        If no sales table is found in the warehouse database.
    Exception
        If there is an error connecting to or querying the database.
    """
    try:
        with sqlite3.connect(DB_PATH) as conn:
            sales_table = _find_table(conn, ["sales", "sale"]) or _find_table(
                conn, ["transactions"]
            )
            product_table = _find_table(conn, ["product", "products", "store"])
            customer_table = _find_table(conn, ["customer", "customers"])

            if not sales_table:
                raise RuntimeError("No sales table found in warehouse")

            sales_df = pd.read_sql_query(f"SELECT * FROM {sales_table}", conn)  # noqa: S608
            products_df = (
                pd.read_sql_query(f"SELECT * FROM {product_table}", conn)  # noqa: S608
                if product_table
                else pd.DataFrame()
            )
            customers_df = (
                pd.read_sql_query(f"SELECT * FROM {customer_table}", conn)  # noqa: S608
                if customer_table
                else pd.DataFrame()
            )

        # Trim whitespace
        sales_df.columns = [c.strip() for c in sales_df.columns]
        products_df.columns = [c.strip() for c in products_df.columns]
        customers_df.columns = [c.strip() for c in customers_df.columns]

        merged = sales_df.copy()

        if not products_df.empty:
            if "product_id" in merged.columns and "product_id" in products_df.columns:
                merged = merged.merge(
                    products_df, on="product_id", how="left", suffixes=("", "_prod")
                )
            else:
                logger.warning("Product table present but no common 'product_id' key to join on")

        if not customers_df.empty:
            if "customer_id" in merged.columns and "customer_id" in customers_df.columns:
                merged = merged.merge(
                    customers_df, on="customer_id", how="left", suffixes=("", "_cust")
                )
            else:
                logger.warning("Customer table present but no common 'customer_id' key to join on")

        return merged

    except Exception as e:
        logger.error(f"Error loading warehouse data from {DB_PATH}: {e}")
        raise


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:  # noqa: UP045
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _extract_columns(df: pd.DataFrame):
    """Extract relevant column names from the dataframe."""
    sale_date_col = _first_existing_column(
        df, ["sale_date", "SaleDate", "date", "saleDate", "transaction_date"]
    )
    units_col = _first_existing_column(
        df, ["units", "quantity", "quantity_sold", "units_sold", "qty"]
    )
    sale_amount_col = _first_existing_column(
        df, ["sale_amount", "SaleAmount", "amount", "gross_amount", "saleamount", "total"]
    )
    cogs_col = _first_existing_column(df, ["cogs", "cost_of_goods_sold", "cost", "unit_cost"])
    product_name_col = _first_existing_column(
        df, ["product_name", "product", "name"]
    ) or _first_existing_column(df, ["product_name_prod", "name_prod"])
    product_unitprice_col = _first_existing_column(
        df, ["unitprice", "unit_price", "price", "unitPrice", "UnitPrice", "price_prod"]
    )
    region_col = _first_existing_column(
        df, ["region", "customer_region", "customerregion", "region_name", "region_cust"]
    ) or _first_existing_column(df, ["region_prod"])
    return (
        sale_date_col,
        units_col,
        sale_amount_col,
        cogs_col,
        product_name_col,
        product_unitprice_col,
        region_col,
    )


def _add_sale_quarter(df, sale_date_col):
    if sale_date_col:
        df["_sale_date_dt"] = pd.to_datetime(df[sale_date_col], errors="coerce")
        n_bad = df["_sale_date_dt"].isna().sum()
        if n_bad:
            logger.warning(f"{n_bad} sale rows have invalid or null dates")
        df["sale_quarter"] = df["_sale_date_dt"].dt.to_period("Q").astype(str)  # type: ignore
    else:
        df["sale_quarter"] = pd.NA
    return df


def _add_units_sold(df, units_col):
    if units_col:
        df["units_sold"] = pd.to_numeric(df[units_col], errors="coerce").fillna(0)
    else:
        df["units_sold"] = 1
    return df


def _add_sales_revenue(df, sale_amount_col, product_unitprice_col):
    if sale_amount_col:
        df["sales_revenue"] = pd.to_numeric(df[sale_amount_col], errors="coerce").fillna(0)
    elif product_unitprice_col:
        df["sales_revenue"] = (
            pd.to_numeric(df[product_unitprice_col], errors="coerce").fillna(0) * df["units_sold"]
        )
    else:
        df["sales_revenue"] = 0
    return df


def _add_cogs_total(df, product_unitprice_col, cogs_col):
    if product_unitprice_col:
        df["cogs_total"] = (
            pd.to_numeric(df[product_unitprice_col], errors="coerce").fillna(0) * df["units_sold"]
        )
    elif cogs_col:
        df["cogs_total"] = pd.to_numeric(df[cogs_col], errors="coerce").fillna(0)
    else:
        df["cogs_total"] = np.nan
    return df


def _add_gross_profit(df):
    df["gross_profit"] = df["sales_revenue"] - df["cogs_total"]
    return df


def _add_product_name(df, product_name_col):
    if product_name_col and product_name_col in df.columns:
        df["product_name"] = df[product_name_col].astype(str)
    elif "product_id" in df.columns:
        df["product_name"] = df["product_id"].astype(str)
    else:
        df["product_name"] = None
    return df


def _add_region(df, region_col):
    if region_col and region_col in df.columns:
        df["region"] = (
            df[region_col]
            .astype(str)
            .str.strip()
            .replace({"nan": pd.NA})
            .str.replace(r"[_\-].*", "", regex=True)  # Extract first word before underscore or dash
            .str.title()
        )
    elif "customer_region" in df.columns:
        df["region"] = (
            df["customer_region"]
            .astype(str)
            .str.strip()
            .replace({"nan": pd.NA})
            .str.replace(r"[_\-].*", "", regex=True)  # Extract first word before underscore or dash
            .str.title()
        )
    else:
        df["region"] = pd.NA

    # Drop rows where region is empty or NA
    df = df.dropna(subset=["region"])
    return df[df["region"] != ""]


def _prepare_dataframe(
    df: pd.DataFrame,
    sale_date_col,
    units_col,
    sale_amount_col,
    cogs_col,
    product_name_col,
    product_unitprice_col,
    region_col,
):
    """Prepare the dataframe by adding calculated columns."""
    df = _add_sale_quarter(df, sale_date_col)
    df = _add_units_sold(df, units_col)
    df = _add_sales_revenue(df, sale_amount_col, product_unitprice_col)
    df = _add_cogs_total(df, product_unitprice_col, cogs_col)
    df = _add_gross_profit(df)
    df = _add_product_name(df, product_name_col)
    return _add_region(df, region_col)


def _aggregate_cube(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate the dataframe to build the OLAP cube."""
    group_cols = ["product_name", "region", "sale_quarter"]
    agg = {"units_sold": "sum", "sales_revenue": "sum", "cogs_total": "sum", "gross_profit": "sum"}
    cube = df.groupby(group_cols, dropna=False, as_index=False).agg(agg)

    cube["average_selling_price"] = cube.apply(
        lambda r: (r["sales_revenue"] / r["units_sold"])
        if r["units_sold"] not in (0, np.nan)
        else np.nan,
        axis=1,
    )
    cube["average_gross_profit"] = cube.apply(
        lambda r: (r["gross_profit"] / r["units_sold"])
        if r["units_sold"] not in (0, np.nan)
        else np.nan,
        axis=1,
    )

    return cube.rename(columns={"sales_revenue": "total_sales_revenue", "cogs_total": "total_cogs"})


def _compute_growth_and_finalize(cube: pd.DataFrame) -> pd.DataFrame:
    """Compute sales growth and finalize the cube columns."""
    # Compute sales growth percent (QoQ) per product+region
    try:
        cube["_sale_quarter_period"] = pd.PeriodIndex(cube["sale_quarter"], freq="Q")  # type: ignore
        sort_by = ["product_name", "region", "_sale_quarter_period"]
    except Exception:
        cube["_sale_quarter_period"] = pd.NA
        sort_by = ["product_name", "region", "sale_quarter"]

    cube = cube.sort_values(sort_by).reset_index(drop=True)

    cube["sales_growth_pct"] = (
        cube.groupby(["product_name", "region"])["total_sales_revenue"]
        .apply(lambda s: s.pct_change() * 100)
        .reset_index(level=[0, 1], drop=True)
    )

    # Round specified numeric columns and growth
    numeric_cols = [
        "units_sold",
        "total_sales_revenue",
        "total_cogs",
        "gross_profit",
        "average_selling_price",
        "average_gross_profit",
        "sales_growth_pct",
    ]
    for c in numeric_cols:
        if c in cube.columns:
            cube[c] = pd.to_numeric(cube[c], errors="coerce").round(2)

    # Replace NaN growth with 0 for first-observation growths
    if "sales_growth_pct" in cube.columns:
        cube["sales_growth_pct"] = cube["sales_growth_pct"].fillna(0)

    desired_cols = [
        "product_name",
        "region",
        "sale_quarter",
        "units_sold",
        "total_sales_revenue",
        "sales_growth_pct",
        "total_cogs",
        "gross_profit",
        "average_selling_price",
        "average_gross_profit",
    ]
    for c in desired_cols:
        if c not in cube.columns:
            cube[c] = pd.NA

    return cube[desired_cols]


def create_olap_cube() -> pd.DataFrame:
    """Build a multidimensional OLAP cube aggregated by product, region, and quarter.

    Loads sales, product, and customer data from the warehouse, prepares and aggregates
    the data, computes sales growth, and writes the result to a CSV file.

    Returns
    -------
    pd.DataFrame
        The OLAP cube dataframe with aggregated and calculated metrics.

    Raises
    ------
    Exception
        If there is an error during cube creation or writing output.
    """
    df = ingest_warehouse()
    if df.empty:
        logger.warning("No data available from warehouse to build OLAP cube")
        return pd.DataFrame()

    (
        sale_date_col,
        units_col,
        sale_amount_col,
        cogs_col,
        product_name_col,
        product_unitprice_col,
        region_col,
    ) = _extract_columns(df)

    df = _prepare_dataframe(
        df,
        sale_date_col,
        units_col,
        sale_amount_col,
        cogs_col,
        product_name_col,
        product_unitprice_col,
        region_col,
    )

    cube = _aggregate_cube(df)
    cube = _compute_growth_and_finalize(cube)

    try:
        cube.to_csv(CUBED_FILE, index=False)
        logger.info(f"OLAP cube written to {CUBED_FILE} rows={len(cube)}")
    except Exception as e:
        logger.error(f"Failed to write OLAP cube to {CUBED_FILE}: {e}")

    return cube


if __name__ == "__main__":
    create_olap_cube()
