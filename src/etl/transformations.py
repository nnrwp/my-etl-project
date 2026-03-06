from pyspark.sql.functions import col, when

def add_temperature_status(df):
    """Add status column based on temperature."""
    return df.withColumn("status",
        when(col("temperature") > 30, "HOT")
        .when(col("temperature") < 10, "COLD")
        .otherwise("NORMAL")
    )

def filter_valid_readings(df):
    """Remove invalid temperature readings."""
    return df.filter(
        (col("temperature") >= -50) &
        (col("temperature") <= 100)
    )

def celsius_to_fahrenheit(df):
    """Convert temperature from Celsius to Fahrenheit."""
    return df.withColumn("temp_f", col("temperature") * 9/5 + 32)

