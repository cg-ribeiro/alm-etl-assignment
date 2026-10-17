"""Derived columns of the final table: AmountEUR, ClientSecuredIND, EnterprizeSize, SecuredAmountEUR."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from alm_etl.config import BASE_CURRENCY, SECURED_AMOUNT_CAP_EUR

AMOUNT_TYPE = "decimal(15,2)"
RATE_TYPE = "decimal(18,8)"


def add_amount_eur(df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
    """Add AmountEUR = OriginalAmount * rate matched on currency and snapshot date (EUR rate is 1)."""
    rates = exchange_rates_df.select(
        F.col("Currency").alias("rate_currency"),
        F.col("SnapshotDate").try_cast("date").alias("rate_snapshot_date"),
        F.col("ExchangeRate").try_cast(RATE_TYPE).alias("rate"),
    )
    joined = df.join(
        rates,
        (df["OriginalCurrency"] == rates["rate_currency"])
        & (df["SnapshotDate"] == rates["rate_snapshot_date"]),
        "left",
    )
    rate = F.when(F.col("OriginalCurrency") == BASE_CURRENCY, F.lit(1)).otherwise(F.col("rate"))
    return joined.withColumn(
        "AmountEUR", (F.col("OriginalAmount") * rate).cast(AMOUNT_TYPE)
    ).drop("rate_currency", "rate_snapshot_date", "rate")


def add_client_secured_ind(df: DataFrame, client_secured_df: DataFrame) -> DataFrame:
    """Add ClientSecuredIND from the lookup (Y -> true, N -> false, unknown client -> null)."""
    secured = client_secured_df.select(
        F.col("ClientNumber").try_cast("int").alias("ClientNumber"),
        F.when(F.col("ClientSecuredInd") == "Y", True)
        .when(F.col("ClientSecuredInd") == "N", False)
        .alias("ClientSecuredIND"),
    ).dropDuplicates(["ClientNumber"])
    return df.join(secured, on="ClientNumber", how="left")


def add_enterprise_size(df: DataFrame) -> DataFrame:
    """Add EnterprizeSize: S (<100), M (100-999), L (>=1000) or NA when employees are unknown."""
    employees = F.col("NumberOfEmployees")
    return df.withColumn(
        "EnterprizeSize",
        F.when(employees < 100, "S")
        .when(employees < 1000, "M")
        .when(employees >= 1000, "L")
        .otherwise("NA"),
    )


def add_secured_amount(df: DataFrame, cap: int = SECURED_AMOUNT_CAP_EUR) -> DataFrame:
    """Allocate the per (SnapshotDate, ClientGroup) cap top-down by ClientNumber to secured clients.

    Secured clients that receive nothing are flagged as not secured.
    """
    earlier_rows = (
        Window.partitionBy("SnapshotDate", "ClientGroup")
        .orderBy("ClientNumber")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    is_secured = F.coalesce(F.col("ClientSecuredIND"), F.lit(False))
    secured_amount = F.when(is_secured, F.coalesce(F.col("AmountEUR"), F.lit(0))).otherwise(0)
    remaining_cap = F.greatest(
        F.lit(cap) - F.coalesce(F.sum("secured_amount").over(earlier_rows), F.lit(0)), F.lit(0)
    )
    return (
        df.withColumn("secured_amount", secured_amount)
        .withColumn(
            "SecuredAmountEUR",
            F.when(is_secured, F.least(F.col("secured_amount"), remaining_cap))
            .otherwise(0)
            .cast(AMOUNT_TYPE),
        )
        .withColumn(
            "ClientSecuredIND",
            F.when(
                F.col("ClientSecuredIND") & (F.col("SecuredAmountEUR") == 0), False
            ).otherwise(F.col("ClientSecuredIND")),
        )
        .drop("secured_amount")
    )
