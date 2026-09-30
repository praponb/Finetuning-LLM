from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2023)
    .select(
        "customer_id",
        "status",
        F.col("discount").cast("int").alias("discount_int"),
        F.round("amount", 2).alias("amount_rounded"),
    )
)
