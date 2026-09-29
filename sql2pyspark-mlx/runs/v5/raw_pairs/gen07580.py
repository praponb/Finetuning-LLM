from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-07-01")
    .rollup("region", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .orderBy("region")
)
