from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        F.round((F.col("discount") / F.col("amount")), 2).alias("discount_per_amount"),
    )
)
