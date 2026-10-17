"""Reading source files and writing Delta tables.

This is the only module that touches storage, so it is also the only place with
try/except: Spark is lazy, so transformation errors also surface here, when the
data is read or written. Errors are logged with context (path or table) and
re-raised, so the notebook run still fails.
"""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_source(spark: SparkSession, path: str, fmt: str, options: dict) -> DataFrame:
    """Read a csv or parquet source exactly as it is stored.

    Args:
        spark: Active Spark session.
        path: File or folder path, e.g. a Unity Catalog volume path.
        fmt: Spark reader format ("csv" or "parquet").
        options: Reader options; for csv use inferSchema=false so all columns stay string.

    Returns:
        The source data without any change.
    """
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
    """Write a DataFrame to a Delta table.

    Args:
        df: Data to write.
        table: Fully qualified table name (catalog.schema.table).
        mode: Spark save mode.
        partition_columns: Columns to partition the table by.
        replace_where: Predicate for an overwrite that only replaces the matching rows
            (e.g. the snapshot dates being loaded). Without it, the whole table is replaced.
    """
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
