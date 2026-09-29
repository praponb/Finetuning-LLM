from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .filter(~F.col("o.channel").isin(["store", "partner"]))
    .groupBy(F.col("o.status"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.method")))).alias("method_values"),
        F.countDistinct(F.col("p.amount")).alias("unique_methods"),
    )
)
