from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.credit_limit") < 12200)
    .groupBy(F.col("c.id"), F.col("c.name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
)
