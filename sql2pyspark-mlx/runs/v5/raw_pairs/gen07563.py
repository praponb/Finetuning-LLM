from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") > "2025-07-01") & F.col("order_id").isin([20, 37]))
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .orderBy(F.col("max_cost").desc())
)
