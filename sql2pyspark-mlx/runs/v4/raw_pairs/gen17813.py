from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        F.when(F.col("sqft") >= 16638, "high").when(F.col("sqft") >= 7142, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("stddev_sqft") >= 71)
    .select("opened_month", "sqft_bucket", "stddev_sqft", "row_count")
    .orderBy(F.col("stddev_sqft").desc())
)
