"""Read source files and write Delta tables."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_source(spark: SparkSession, path: str, fmt: str, options: dict) -> DataFrame:
    """Read a csv or parquet file with all columns as string."""
    try:
        return spark.read.format(fmt).options(**options).load(path)
    except Exception:
        logger.exception("Failed to read %s source from %s", fmt, path)
        raise


def write_table(
    df: DataFrame,
    table: str,
    mode: str = "overwrite",
    partition_columns: list[str] | None = None,
    replace_where: str | None = None,
) -> None:
    """Write a DataFrame to a Delta table, optionally replacing only the rows matching replace_where."""
    writer = df.write.format("delta").mode(mode)
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    if replace_where:
        writer = writer.option("replaceWhere", replace_where)
    elif mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    try:
        writer.saveAsTable(table)
        logger.info("Wrote table %s (mode=%s)", table, mode)
    except Exception:
        logger.exception("Failed to write table %s", table)
        raise
