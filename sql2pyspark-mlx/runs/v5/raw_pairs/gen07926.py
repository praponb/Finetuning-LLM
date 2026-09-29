from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-04-01", "2024-05-01") & F.col("sqft").between(6508, 10235))
    .groupBy("city")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"),
    )
    .orderBy(F.col("east_count").desc(), F.col("stddev_sqft").desc())
)
