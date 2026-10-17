from datetime import date
from decimal import Decimal

from alm_etl.derived_columns import (
    add_amount_eur,
    add_client_secured_ind,
    add_enterprise_size,
    add_secured_amount,
)

SNAPSHOT = date(2023, 2, 6)


def test_add_amount_eur_converts_with_rate_of_same_snapshot(spark):
    df = spark.createDataFrame(
        [(1, Decimal("100.00"), "EUR", SNAPSHOT),
         (2, Decimal("100.00"), "USD", SNAPSHOT),
         (3, Decimal("100.00"), "JPY", SNAPSHOT)],
        "ClientNumber int, OriginalAmount decimal(15,2), OriginalCurrency string, SnapshotDate date",
    )
    rates = spark.createDataFrame(
        [("0", "USD", "0.9873", "2023-02-06"), ("1", "USD", "0.5", "2023-02-07")],
        "_c0 string, Currency string, ExchangeRate string, SnapshotDate string",
    )

    result = {r.ClientNumber: r.AmountEUR for r in add_amount_eur(df, rates).collect()}

    assert result == {1: Decimal("100.00"), 2: Decimal("98.73"), 3: None}


def test_add_client_secured_ind_maps_lookup_and_dedupes(spark):
    df = spark.createDataFrame([(664521,), (901,), (12,), (7,)], "ClientNumber int")
    lookup = spark.createDataFrame(
        [("664521", "Y"), ("901", "N"), ("12", "Y"), ("12", "Y")],
        "ClientNumber string, ClientSecuredInd string",
    )

    result = add_client_secured_ind(df, lookup).collect()

    assert len(result) == 4
    assert {r.ClientNumber: r.ClientSecuredIND for r in result} == {
        664521: True, 901: False, 12: True, 7: None,
    }


def test_add_enterprise_size_boundaries(spark):
    df = spark.createDataFrame([(99,), (100,), (999,), (1000,), (None,)], "NumberOfEmployees int")

    result = [r.EnterprizeSize for r in add_enterprise_size(df).orderBy("NumberOfEmployees").collect()]

    assert result == ["NA", "S", "M", "M", "L"]


def test_add_secured_amount_matches_mapping_example(spark):
    df = spark.createDataFrame(
        [(5, Decimal("10000"), True), (2, Decimal("455000"), True), (1, Decimal("100000"), False),
         (55, Decimal("250000"), True), (89, Decimal("122300"), True), (3, Decimal("23723"), None)],
        "ClientNumber int, AmountEUR decimal(15,2), ClientSecuredIND boolean",
    ).selectExpr("*", "'1' AS ClientGroup", "DATE'2023-02-06' AS SnapshotDate")

    result = {
        r.ClientNumber: (r.SecuredAmountEUR, r.ClientSecuredIND)
        for r in add_secured_amount(df).collect()
    }

    assert result == {
        5: (Decimal("10000.00"), True),
        2: (Decimal("455000.00"), True),
        1: (Decimal("0.00"), False),
        55: (Decimal("35000.00"), True),
        89: (Decimal("0.00"), False),
        3: (Decimal("0.00"), None),
    }
