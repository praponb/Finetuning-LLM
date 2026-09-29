from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.date_sub("order_date", 14).alias("order_date_minus_14d"),
        F.when(F.col("amount") >= 1340, "High").when(F.col("amount") >= 510, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .groupBy("channel")
    .pivot("status", ["CANCELLED", "RETURNED"])
    .agg(F.avg("shipping_fee").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "channel",
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_cnt").alias("cancelled_cnt"),
        F.col("RETURNED_amount").alias("returned_amount"),
        F.col("RETURNED_cnt").alias("returned_cnt"),
    )
)
