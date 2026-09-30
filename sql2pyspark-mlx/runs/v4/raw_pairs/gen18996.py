from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.round((F.col("amount") * 1.1), 2).alias("amount_adjusted"),
        F.round("shipping_fee", 1).alias("shipping_fee_rounded"),
    )
)

result = (
    valid
    .select(
        "id",
        "store_id",
        F.col("amount").alias("AMOUNT"),
        F.col("shipping_fee_rounded").alias("SHIPPING_FEE_ROUNDED"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("amount_adjusted").alias("AMOUNT_ADJUSTED_VAL"),
    )
    .unpivot(
        ["id", "store_id"],
        ["AMOUNT", "SHIPPING_FEE_ROUNDED", "SHIPPING_FEE_VAL", "AMOUNT_ADJUSTED_VAL"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
