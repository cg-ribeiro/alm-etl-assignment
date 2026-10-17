# ALM ETL assignment

A PySpark ETL for Databricks. It loads two client sources and two lookup files, then builds one
typed final table. The mapping from `MappingList.xlsx` is written once in
`src/alm_etl/config.py`, and generic transformation functions apply it.

## Architecture

```
Volume /Volumes/workspace/alm/raw
  Source Files/Source1.csv, Source Files/Source2/ (parquet)
  Lookup Files/Exchange_Rates.csv, Lookup Files/Client_Secured_Ind.csv
        |
        |  01_load_raw_and_lookups  (stored unchanged, all columns string)
        v
  raw_source1, raw_source2, lookup_exchange_rates, lookup_client_secured_ind
        |
        |  02_build_final  (map -> Y/N/NA -> cast -> union -> derived columns)
        v
  final_client_amounts  (Delta, partitioned by SnapshotDate)
        |
        |  03_results  (SQL answers)
```

All tables live in `workspace.alm`.

## Repository layout

```
src/alm_etl/
  config.py            paths, table names, source mappings, final schema (types + defaults)
  io.py                read source files, write Delta tables (the only try/except)
  transformations.py   apply_mapping, normalize_yes_no, cast_to_final_schema, union_sources
  derived_columns.py   add_amount_eur, add_client_secured_ind, add_enterprise_size, add_secured_amount
  pipeline.py          map_source, build_final (puts the steps together)
notebooks/             Databricks source notebooks
  01_load_raw_and_lookups.py
  02_build_final.py
  03_results.py
  run_tests.py
tests/                 pytest unit tests on small hand-built DataFrames
requirements-dev.txt   pyspark 4.0.1, pytest
```

The data files are not committed. `.gitignore` excludes `*.csv`, `*.parquet` and `*.xlsx`.

## How to run

### Databricks (Free Edition)

1. Add this GitHub repository as a **Git folder** in the workspace.
2. Create the volume `workspace.alm.raw` (Catalog > workspace > create schema `alm` > create volume `raw`).
   Upload the `Source Files` and `Lookup Files` folders and keep their folder names:
   `/Volumes/workspace/alm/raw/Source Files/...` and `/Volumes/workspace/alm/raw/Lookup Files/...`.
3. Run the notebooks in order: `01_load_raw_and_lookups`, `02_build_final`, `03_results`.
4. Optional: run `run_tests` to run the pytest suite on Databricks.

Notebook 01 runs `CREATE SCHEMA IF NOT EXISTS workspace.alm`. The volume and the uploaded files must
exist before notebook 01 can read them. The notebooks import the package with
`sys.path.append(os.path.abspath("../src"))`. The code uses `Column.try_cast`, which needs
PySpark 4 (serverless or DBR 17+).

### Locally

Requires Java 17+.

```bash
pip install -r requirements-dev.txt
pytest -q
```

`tests/conftest.py` starts a local Spark session with ANSI mode on, the same as Databricks serverless.
If a session is already running (for example in the `run_tests` notebook), the fixture reuses it and
does not stop it.

## Design decisions

- **Config-driven, generic mapping.** Each source has a `column_mapping` (source column -> final column).
  `FINAL_SCHEMA` lists every final column with its type and default. The same functions handle both
  sources, so a new source only needs a new config entry.
- **Raw tables are unchanged.** Files are read with `inferSchema=false`, so every column is a string.
  This includes the unnamed index column of Source1 and Exchange_Rates, which Spark names `_c0`. All
  typing and cleaning happens in the final step, so the raw layer can always be reprocessed.
- **ANSI-safe casting.** Databricks serverless runs with ANSI mode on, where a normal `cast` fails on bad
  input. `cast_to_final_schema` uses `try_cast`, so invalid values become null (or the column default).
- **Partitioned by SnapshotDate.** The data is loaded and queried per snapshot, and the cap logic is
  computed per snapshot. Notebook 02 overwrites only the snapshot dates it loads (`replaceWhere`
  `SnapshotDate IN (...)`). Earlier snapshots stay, and a rerun gives the same result. The first run
  is a plain overwrite because the table does not exist yet.
- **try/except only around I/O.** Only `io.py` reads files and writes tables, and that is where the
  try/except blocks are. They log the path or table with `logging` and re-raise. The transformations
  are pure functions (DataFrame in, DataFrame out) with no exception handling.
- **Lean tests.** There is one focused test per behaviour. The cap test uses the example from the mapping file.

## Business rules as implemented

| Column | Rule |
|---|---|
| ClientNumber | `try_cast` to int, which also removes leading zeros (`000664521` -> 664521) |
| OnboardingDate, SnapshotDate | `try_cast` to date. Invalid values become null |
| LoanCheck, DiscountCheck | Yes/No -> Y/N. Anything else, or a column the source does not have, becomes `NA` |
| Country | null for Source2 (the column is not in that source) |
| AmountEUR | `OriginalAmount * rate`, joined on Currency and SnapshotDate. EUR = 1. No rate -> null. decimal(15,2) |
| ClientSecuredIND | From the lookup: Y -> true, N -> false. Lookup ClientNumber is cast to int and deduplicated. Client not in the lookup -> null |
| EnterprizeSize | <100 `S`, 100-999 `M`, >=1000 `L`, null `NA` |
| SecuredAmountEUR | Per (SnapshotDate, ClientGroup), ordered by ClientNumber: `least(AmountEUR, greatest(500000 - sum of earlier secured amounts, 0))` for secured rows, 0 for other rows. A secured row that gets 0 has its ClientSecuredIND set to false |

## Data quality issues and assumptions

- **Leading zeros** in ClientNumber are removed by the int cast.
- **Invalid date** `2010-05-111` becomes null.
- **Duplicate client 12** in `Client_Secured_Ind` is deduplicated. Both rows are `Y`. If the values
  conflicted, `dropDuplicates` would keep one of them arbitrarily.
- **EUR** is not in the exchange-rate lookup, so its rate is set to 1.
- **Clients missing from the lookup** keep ClientSecuredIND null and get SecuredAmountEUR 0. Q4 filters on
  `ClientSecuredIND = false`, so these rows are not counted there (clients 5 and 45).
- **A secured client with no exchange rate** (AmountEUR null) counts as 0 in the cap allocation. It gets
  SecuredAmountEUR 0 and ClientSecuredIND false. This does not happen in the data.
- **Cap window** is per SnapshotDate and ClientGroup, so each snapshot has its own cap. The order uses
  ClientNumber as a number. ClientGroup stays a string, as `FINAL_SCHEMA` specifies.
- **Column name** is `SecuredAmountEUR`, as in the Mapping tab. The logic tab calls it SecuredAmount.
- **`EnterprizeSize`** keeps the spelling from the mapping.
- **Typo in the mapping example:** 1000 * 0.9987 is 998.7, not 9987.

## Results

These values come from `notebooks/03_results.py`. I checked them with a separate pandas calculation
and got the same numbers.

| # | Question | Answer |
|---|---|---|
| 1 | SecuredAmountEUR for ClientNumber 6991 | 0.00 |
| 2 | Number of records with EnterprizeSize `M` | 6 |
| 3 | Total AmountEUR for SourceSystem `Source2` | 1,800,631.68 |
| 4 | Total OriginalAmount where ClientSecuredIND is false | 312,301.56 |
