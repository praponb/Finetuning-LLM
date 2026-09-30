from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "shipping_fee",
        F.date_format("order_date", "yyyy-MM-dd").alias("order_date_label"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
