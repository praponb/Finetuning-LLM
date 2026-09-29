from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("stddev_sqft") > 21)
    .orderBy("stddev_sqft", "min_opened_date")
)
