from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("amount").alias("AMOUNT"),
        "shipping_fee",
        F.col("discount").alias("DISCOUNT_VAL"),
    )
    .unpivot(
        ["id", "store_id"],
        ["AMOUNT", "shipping_fee", "DISCOUNT_VAL"],
        "kpi",
        "metric_value",
    )
)
