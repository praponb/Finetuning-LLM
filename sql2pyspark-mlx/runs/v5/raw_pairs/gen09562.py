from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "order_id",
        "status",
        F.when(F.col("carrier").isin(["Flash", "Kerry"]), "priority").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
    .orderBy(F.col("stddev_cost").desc())
)
