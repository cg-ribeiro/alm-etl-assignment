"""The four derived columns of the final table (see the logic tabs in MappingList.xlsx).

Each function expects the mapped and typed columns (ClientNumber int, OriginalAmount
decimal, SnapshotDate date, ...) and adds one derived column.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from alm_etl.config import BASE_CURRENCY, SECURED_AMOUNT_CAP_EUR

AMOUNT_TYPE = "decimal(15,2)"
RATE_TYPE = "decimal(18,8)"


def add_amount_eur(df: DataFrame, exchange_rates_df: DataFrame) -> DataFrame:
    """Add AmountEUR = OriginalAmount * exchange rate.

    The rate is matched on currency AND snapshot date, because rates change every day.
    EUR is not in the lookup and gets rate 1. A missing rate gives AmountEUR null (the default).

    Args:
        df: Mapped data with OriginalAmount, OriginalCurrency and SnapshotDate.
        exchange_rates_df: Raw Exchange_Rates lookup (all columns string).
    """
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
    """Add ClientSecuredIND from the Client_Secured_Ind lookup.

    Y -> true, N -> false, client not in the lookup -> null (the default).
    The lookup's ClientNumber is cast to int to match the final column, and duplicate
    clients are removed so the join cannot duplicate rows (client 12 appears twice).

    Args:
        df: Mapped data with ClientNumber.
        client_secured_df: Raw Client_Secured_Ind lookup (all columns string).
    """
    secured = client_secured_df.select(
        F.col("ClientNumber").try_cast("int").alias("ClientNumber"),
        F.when(F.col("ClientSecuredInd") == "Y", True)
        .when(F.col("ClientSecuredInd") == "N", False)
        .alias("ClientSecuredIND"),
    ).dropDuplicates(["ClientNumber"])
    return df.join(secured, on="ClientNumber", how="left")


def add_enterprise_size(df: DataFrame) -> DataFrame:
    """Add EnterprizeSize: "S" (<100 employees), "M" (100-999), "L" (>=1000), "NA" if unknown."""
    employees = F.col("NumberOfEmployees")
    return df.withColumn(
        "EnterprizeSize",
        F.when(employees < 100, "S")
        .when(employees < 1000, "M")
        .when(employees >= 1000, "L")
        .otherwise("NA"),
    )


def add_secured_amount(df: DataFrame, cap: int = SECURED_AMOUNT_CAP_EUR) -> DataFrame:
    """Add SecuredAmountEUR by allocating a cap per client group to its secured clients.

    Per SnapshotDate and ClientGroup, rows are ordered by ClientNumber. Each secured row
    gets its AmountEUR, limited to what is left of the cap after the earlier secured rows.
    Rows that are not secured (false or null) get 0. Secured rows that end up with 0 are
    set to ClientSecuredIND = false.

    Args:
        df: Data with ClientGroup, ClientNumber, SnapshotDate, AmountEUR and ClientSecuredIND.
        cap: Maximum secured amount in EUR per client group.
    """
    # All rows before the current one in the same group.
    earlier_rows = (
        Window.partitionBy("SnapshotDate", "ClientGroup")
        .orderBy("ClientNumber")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    is_secured = F.coalesce(F.col("ClientSecuredIND"), F.lit(False))
    # Amount that counts towards the cap; null AmountEUR counts as 0, because least()
    # ignores nulls and would otherwise give the whole remaining cap to that row.
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
