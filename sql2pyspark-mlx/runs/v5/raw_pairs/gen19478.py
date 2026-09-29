from pyspark.sql import functions as F

base = df_orders.filter(F.col("amount").isNotNull())

agg = (
    base
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.min("discount").alias("min_discount"),
        F.max("order_date").alias("last_order_date"),
    )
)

result = (
    agg
    .select("order_month", "min_discount", "last_order_date")
    .orderBy("min_discount")
)
