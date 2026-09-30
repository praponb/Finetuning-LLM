from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry"]) & F.col("carrier").isNull() & F.col("carrier").isin(["DHL", "UPS"]))
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
    .orderBy("min_delivered_date")
)
