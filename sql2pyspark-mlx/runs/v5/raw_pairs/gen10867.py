from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") < "2025-03-28")
    .groupBy(F.col("o.customer_id"))
    .agg(
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
)
