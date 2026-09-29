from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.sort_array(F.collect_set(F.col("p.method"))).alias("method_list"),
    )
    .orderBy("status", F.col("avg_amount").desc())
)
