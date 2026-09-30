from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("amount").between(410, 770) & (F.col("order_date") >= "2023-08-04")) | F.col("amount").between(840, 1390))
    .withColumn("greatest_discount_shipping_fee", F.greatest("discount", "shipping_fee"))
)

order_month_summary = (
    base
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.sum("greatest_discount_shipping_fee").alias("total_greatest_discount_shipping_fee"),
    )
)

result = (
    order_month_summary
    .filter(F.col("total_greatest_discount_shipping_fee") < 300)
    .orderBy("order_month")
)
