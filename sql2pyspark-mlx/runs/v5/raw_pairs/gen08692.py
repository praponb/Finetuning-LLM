from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.status") == "RETURNED")
    .groupBy(F.col("o.store_id"))
    .agg((F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"))
    .orderBy(F.col("amount_range").desc())
)
