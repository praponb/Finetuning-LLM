from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "London")
    .groupBy(
        F.month("opened_date").alias("opened_month"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") >= 26)
    .orderBy("stddev_sqft")
)
