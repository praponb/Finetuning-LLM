from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-11-05").cast("date"))
    .select(
        "*",
        F.round((F.col("amount") * 0.85), 2).alias("adjusted_amount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        F.col("shipping_fee").alias("SHIPPING_FEE"),
        F.col("amount").alias("AMOUNT"),
        F.col("adjusted_amount").alias("ADJUSTED_AMOUNT_VAL"),
    )
    .unpivot(
        ["id", "customer_id"],
        ["SHIPPING_FEE", "AMOUNT", "ADJUSTED_AMOUNT_VAL"],
        "metric",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
