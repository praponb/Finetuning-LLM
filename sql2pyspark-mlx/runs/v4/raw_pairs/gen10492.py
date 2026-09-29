from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull() & F.col("amount").between(1180, 1370))
    .withColumn("order_date_month_end", F.last_day("order_date"))
)

status_summary = (
    recent_orders
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = (
    status_summary
    .select("status", "pct_shipped")
    .orderBy("status")
)
