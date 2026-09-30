from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([8, 10, 12]))
    .select(
        "id",
        "customer_id",
        "discount",
        F.date_trunc("MONTH", "order_date").alias("order_date_period"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
    .orderBy("discount_filled")
)
