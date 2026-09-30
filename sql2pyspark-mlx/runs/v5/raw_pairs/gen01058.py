from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["East", "West", "Central"]))
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.status").isin(["SHIPPED", "RETURNED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
    )
    .orderBy("latest_paid_at")
)
