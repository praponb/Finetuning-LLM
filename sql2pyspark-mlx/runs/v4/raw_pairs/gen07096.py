from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("status", "carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
    )
    .filter(F.col("fedex_cost") > 49)
    .drop("fedex_cost")
    .orderBy(F.col("stddev_cost").desc())
)
