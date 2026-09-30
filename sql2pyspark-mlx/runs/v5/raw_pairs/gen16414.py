from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .filter(F.col("row_count") < 1000)
)
