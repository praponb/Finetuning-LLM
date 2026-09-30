from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "order_date",
        "status",
        (F.col("amount") * F.col("shipping_fee")).alias("amount_with_shipping"),
        (F.col("amount") - F.col("discount")).alias("amount_minus_discount"),
    )
    .orderBy(F.col("order_date").desc(), F.col("amount_with_shipping").desc())
)
