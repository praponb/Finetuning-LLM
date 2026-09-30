from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "amount",
        F.col("discount").alias("DISCOUNT"),
    )
    .unpivot(
        ["id", "store_id"],
        ["shipping_fee", "amount", "DISCOUNT"],
        "metric",
        "value",
    )
)
