from decimal import Decimal

import pytest

from alm_etl.config import MAPPED_COLUMNS, SOURCES
from alm_etl.transformations import (
    apply_mapping,
    cast_to_final_schema,
    normalize_yes_no,
    union_sources,
)


def _source_frame(spark, columns):
    """Build a one-row all-string frame whose values equal the column names."""
    return spark.createDataFrame([tuple(columns)], ", ".join(f"`{c}` string" for c in columns))


def test_apply_mapping_source1_renames_and_defaults_loan_check(spark):
    mapping = SOURCES["Source1"]["column_mapping"]
    row = apply_mapping(_source_frame(spark, ["_c0", *mapping]), mapping).first()

    assert list(row.asDict()) == MAPPED_COLUMNS
    assert row.OriginalAmount == "ClientAmount"
    assert row.Country == "Location"
    assert row.LoanCheck == "NA"


def test_apply_mapping_source2_defaults_country_and_discount_check(spark):
    mapping = SOURCES["Source2"]["column_mapping"]
    row = apply_mapping(_source_frame(spark, list(mapping)), mapping).first()

    assert row.Country is None
    assert row.DiscountCheck == "NA"
    assert row.LoanCheck == "HasLoan"


def test_apply_mapping_fails_when_source_column_is_missing(spark):
    df = spark.createDataFrame([("1",)], "ClientNumber string")

    with pytest.raises(ValueError, match="ClientGroup"):
        apply_mapping(df, {"ClientNumber": "ClientNumber", "ClientGroup": "ClientGroup"})


def test_cast_to_final_schema_casts_safely_and_fills_defaults(spark):
    df = spark.createDataFrame(
        [("000664521", "2010-05-111", "15045.5", None)],
        "ClientNumber string, OnboardingDate string, OriginalAmount string, LoanCheck string",
    )
    row = cast_to_final_schema(df).first()

    assert row.ClientNumber == 664521
    assert row.OnboardingDate is None
    assert row.OriginalAmount == Decimal("15045.50")
    assert row.LoanCheck == "NA"


def test_normalize_yes_no(spark):
    df = spark.createDataFrame([(" yes",), ("No",), ("maybe",), (None,)], "LoanCheck string")
    result = [r.LoanCheck for r in normalize_yes_no(df, ["LoanCheck"]).collect()]

    assert result == ["Y", "N", "NA", "NA"]


def test_union_sources_matches_by_name(spark):
    first = spark.createDataFrame([("a", "1")], "SourceSystem string, ClientNumber string")
    second = spark.createDataFrame([("2", "b")], "ClientNumber string, SourceSystem string")

    rows = union_sources([first, second]).collect()

    assert [(r.SourceSystem, r.ClientNumber) for r in rows] == [("a", "1"), ("b", "2")]
