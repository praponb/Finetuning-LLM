from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([20, 24, 33]) & ((F.col("order_date") > "2025-10-01") | ~F.col("status").isin(["RETURNED", "SHIPPED", "PENDING"])))
    .select(
        "shipping_fee",
        "amount",
        "order_date",
        F.weekofyear("order_date").alias("order_date_weekofyear"),
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)
