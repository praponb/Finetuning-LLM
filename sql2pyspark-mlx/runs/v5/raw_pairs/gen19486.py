from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount") < 600)
    .groupBy(F.col("c.city"))
    .agg(
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
        F.min(F.col("o.order_date")).alias("first_order_date"),
    )
    .orderBy(F.col("first_order_date").desc(), F.col("city").desc())
)
