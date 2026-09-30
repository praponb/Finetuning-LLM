from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 39)
    .select(
        "id",
        "customer_id",
        "order_date",
        "channel",
        (F.col("shipping_fee") / F.nullif("discount", F.lit(0))).alias("shipping_fee_per_discount"),
        (F.col("amount") / F.nullif("discount", F.lit(0))).alias("amount_per_discount"),
    )
    .orderBy("customer_id")
)
