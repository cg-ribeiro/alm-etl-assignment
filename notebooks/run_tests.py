# Databricks notebook source
# MAGIC %md
# MAGIC # Run unit tests
# MAGIC Runs the pytest suite of the repo on the cluster (pytest is preinstalled on Databricks runtimes).

# COMMAND ----------

import os
import sys

import pytest

sys.dont_write_bytecode = True  # avoid writing __pycache__ into the repo
os.chdir(os.path.abspath(".."))  # repo root, where pytest.ini lives

exit_code = pytest.main(["-q", "-p", "no:cacheprovider"])
assert exit_code == 0, f"Tests failed with exit code {exit_code}"
