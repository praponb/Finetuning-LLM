from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select(
        "id",
        "shipping_fee",
        "customer_id",
        "store_id",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)
