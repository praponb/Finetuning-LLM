from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "UPS", "FedEx"]) & (F.col("order_id").isin([4, 9, 11, 40]) | (F.col("cost") > 90)))
    .groupBy("status", "carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
    )
    .filter(F.col("pct_fedex") < 77)
)
