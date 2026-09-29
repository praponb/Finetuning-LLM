from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status") == "PENDING")
    .groupBy(F.col("s.city"))
    .agg(
        F.sum(F.col("o.amount")).alias("total_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
    .orderBy("city")
)
