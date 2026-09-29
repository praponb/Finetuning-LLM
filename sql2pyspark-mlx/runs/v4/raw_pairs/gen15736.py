from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"), "region")
    .agg(
        F.count("*").alias("row_count"),
        F.bit_or("sqft").alias("sqft_bit_or"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
    )
    .filter(F.col("row_count") < 1)
)
