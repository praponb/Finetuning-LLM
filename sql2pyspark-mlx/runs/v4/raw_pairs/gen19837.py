from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
        F.col("amount").alias("AMOUNT_VAL"),
        "discount",
    )
    .unpivot(
        ["id", "channel"],
        ["SHIPPING_FEE_VAL", "AMOUNT_VAL", "discount"],
        "metric_name",
        "metric_value",
    )
)
