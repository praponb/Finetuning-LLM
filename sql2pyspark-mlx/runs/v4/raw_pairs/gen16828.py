from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2025-06-01", "2025-10-01"))
    .select(
        "id",
        "amount",
        "order_date",
        (F.col("amount") / F.nullif("shipping_fee", F.lit(0))).alias("amount_per_shipping_fee"),
    )
)
