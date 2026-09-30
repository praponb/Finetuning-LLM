from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") > "2025-02-01")
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .filter(F.col("stddev_sqft") > 45)
    .select("opened_month", "stddev_sqft", "sqft_range")
)
