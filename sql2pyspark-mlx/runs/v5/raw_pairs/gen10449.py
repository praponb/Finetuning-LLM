from pyspark.sql import functions as F

src = (
    df_orders
    .filter((F.col("shipping_fee") <= 28) | F.col("shipping_fee").between(22, 29))
)

agg = (
    src
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    agg
    .filter(F.col("total_shipping_fee") < 150)
    .orderBy(F.col("total_shipping_fee").desc())
)
