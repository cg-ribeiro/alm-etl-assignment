"""End-to-end build of the final table from the raw sources and lookups."""

from pyspark.sql import DataFrame

from alm_etl.config import SOURCES, YES_NO_COLUMNS
from alm_etl.derived_columns import (
    add_amount_eur,
    add_client_secured_ind,
    add_enterprise_size,
    add_secured_amount,
)
from alm_etl.transformations import (
    apply_mapping,
    cast_to_final_schema,
    normalize_yes_no,
    union_sources,
)


def map_source(df: DataFrame, source_name: str) -> DataFrame:
    """Map one raw source to the typed final columns."""
    mapped = apply_mapping(df, SOURCES[source_name]["column_mapping"])
    return cast_to_final_schema(normalize_yes_no(mapped, YES_NO_COLUMNS))


def build_final(
    source_dfs: dict[str, DataFrame],
    exchange_rates_df: DataFrame,
    client_secured_df: DataFrame,
) -> DataFrame:
    """Map and union all sources, add the derived columns and return the final table."""
    combined = union_sources([map_source(df, name) for name, df in source_dfs.items()])
    with_amounts = add_amount_eur(combined, exchange_rates_df)
    with_secured_ind = add_client_secured_ind(with_amounts, client_secured_df)
    with_derived = add_secured_amount(add_enterprise_size(with_secured_ind))
    return cast_to_final_schema(with_derived)
