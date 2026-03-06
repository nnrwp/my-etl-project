import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from src.etl.transformations import add_temperature_status, filter_valid_readings, celsius_to_fahrenheit

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").getOrCreate()

def test_add_temperature_status(spark):
    input_data = [(35.0,), (25.0,), (5.0,)]
    input_df = spark.createDataFrame(input_data, ["temperature"])
    
    expected_data = [(35.0, "HOT"), (25.0, "NORMAL"), (5.0, "COLD")]
    expected_df = spark.createDataFrame(expected_data, ["temperature", "status"])
    
    result_df = add_temperature_status(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable = True)

def test_filter_valid_readings(spark):
    input_data = [(25.0,), (150.0,), (-100.0,)]
    input_df = spark.createDataFrame(input_data, ["temperature"])
    
    expected_data = [(25.0,)]
    expected_df = spark.createDataFrame(expected_data, ["temperature"])
    
    result_df = filter_valid_readings(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable = True)

def test_celsius_to_fahrenheit(spark):
    # Input: [(0,), (100,)]
    input_data = [(0.0,), (100.0,)]
    input_df = spark.createDataFrame(input_data, ["temperature"])

    # Expected: [(0, 32.0), (100, 212.0)]
    expected_data = [[(0.0, 32.0), (100.0, 212.0)]]
    expected_df = spark.createDataFrame(expected_data, ["temperature", "temp_f"])

    result_df = celsius_to_fahrenheit(input_df)
    assert_df_equality(result_df, expected_df, ignore_nullable = True)

