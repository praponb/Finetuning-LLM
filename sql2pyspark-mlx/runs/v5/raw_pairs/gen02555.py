from pyspark.sql import functions as F

base = (
    df_orders
    .filter((F.col("status") == "CANCELLED") & F.col("shipping_fee").between(14, 26))
)

orders_stats = (
    base
    .groupBy(
        F.quarter("order_date").alias("order_quarter"),
        F.year("order_date").alias("order_year"),
    )
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
)

result = (
    orders_stats
    .select("order_quarter", "order_year", "shipping_fee_count")
    .orderBy("order_quarter", F.col("order_year").desc())
    .limit(10)
)
