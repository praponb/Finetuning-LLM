from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.31, 0.38))
    .select(
        "id",
        "store_id",
        "discount",
        "status",
        F.greatest("discount", "shipping_fee").alias("greatest_discount_shipping_fee"),
        F.last_day("order_date").alias("order_date_month_end"),
    )
    .orderBy(F.col("greatest_discount_shipping_fee").desc())
)
