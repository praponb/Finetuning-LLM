from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .groupBy(F.col("c.id"), F.col("c.email"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
        F.countDistinct(F.col("o.amount")).alias("unique_statuss"),
    )
)
