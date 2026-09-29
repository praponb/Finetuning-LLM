from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("order_date") >= F.lit("2023-02-25").cast("date"))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        F.col("discount").alias("DISCOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["shipping_fee", "DISCOUNT_VAL"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
