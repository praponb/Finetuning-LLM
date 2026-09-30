from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["South", "North", "East"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("region") == "West", F.col("sqft")).otherwise(0)).alias("west_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("west_sqft") > 29)
)
