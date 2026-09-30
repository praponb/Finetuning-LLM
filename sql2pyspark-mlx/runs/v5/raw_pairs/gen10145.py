from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-03-02"))) & (~F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "*",
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        F.col("shipping_fee").alias("SHIPPING_FEE"),
        F.col("discount").alias("DISCOUNT"),
        F.col("amount").alias("AMOUNT_VAL"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["SHIPPING_FEE", "DISCOUNT", "AMOUNT_VAL"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
