from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 37)
    .select(
        "order_date",
        "customer_id",
        F.when(F.col("status") == "CANCELLED", 1).otherwise(0).alias("is_cancelled"),
    )
    .orderBy("is_cancelled", F.col("order_date").desc())
)
