from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") >= "2023-08-10") & F.col("order_date").between("2023-06-24", "2025-05-01"))
    .select(
        "id",
        "status",
        "channel",
        "shipping_fee",
        F.round((F.col("shipping_fee") / F.col("amount")), 2).alias("amount_with_shipping"),
    )
    .orderBy("status")
)
