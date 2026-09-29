from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"), "status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy("status")
)
