from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(~F.col("s.carrier").isin(["DHL", "Flash", "FedEx"]))
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 31, "high").when(F.col("o.shipping_fee") >= 14, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 4)
    .orderBy(F.col("shipping_fee_bucket").desc())
    .limit(1)
)
