from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("shipping_fee").between(14, 21))
    .withColumn("amount_with_shipping", (F.col("amount") * F.col("shipping_fee")))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

result = (
    cleaned
    .select("customer_id", "channel")
    .subtract(
        df_orders
        .filter(F.col("status") == "PENDING")
        .select("customer_id", "channel")
    )
)
