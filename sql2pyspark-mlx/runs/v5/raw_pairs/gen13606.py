from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 10]) & F.col("sqft").between(5861, 9275))
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
)
