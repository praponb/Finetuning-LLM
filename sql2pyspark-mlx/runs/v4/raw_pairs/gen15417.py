from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([2, 5, 8]))
    .select(
        "id",
        "discount",
        "customer_id",
        "channel",
        (F.col("shipping_fee") * F.col("discount")).alias("shipping_fee_times_discount"),
        F.date_trunc("week", "order_date").alias("order_date_period"),
    )
)
