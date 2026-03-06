import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import add_temperature_status, filter_valid_readings

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").getOrCreate()

def test_add_temperature_status(spark):
    input_data = [(35.0,), (25.0,), (5.0,)]
    input_df = spark.createDataFrame(input_data, ["temperature"])
    
    expected_data = [(35.0, "HOT"), (25.0, "NORMAL"), (5.0, "COLD")]
    expected_df = spark.createDataFrame(expected_data, ["temperature", "status"])
    
    result_df = add_temperature_status(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable = True))

def test_filter_valid_readings(spark):
    input_data = [(25.0,), (150.0,), (-100.0,)]
    input_df = spark.createDataFrame(input_data, ["temperature"])
    
    expected_data = [(25.0,)]
    expected_df = spark.createDataFrame(expected_data, ["temperature"])
    
    result_df = filter_valid_readings(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable = True)