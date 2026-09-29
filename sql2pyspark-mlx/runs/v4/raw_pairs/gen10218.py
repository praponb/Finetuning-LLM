from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        F.col("discount").alias("DISCOUNT_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ["id", "customer_id"],
        ["DISCOUNT_VAL", "AMOUNT_VAL", "SHIPPING_FEE_VAL"],
        "metric",
        "value",
    )
)
