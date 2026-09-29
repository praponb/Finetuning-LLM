from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 38, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 64)
)
