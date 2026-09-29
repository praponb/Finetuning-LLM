from pyspark.sql import functions as F

recent_orders = df_orders.filter(F.col("store_id").isNotNull())

status_summary = (
    recent_orders
    .groupBy("status")
    .agg((F.max("discount") - F.min("discount")).alias("discount_range"))
)

result = (
    status_summary
    .filter(F.col("discount_range") < 63)
    .select("status", "discount_range")
    .orderBy("status")
    .limit(50)
)
