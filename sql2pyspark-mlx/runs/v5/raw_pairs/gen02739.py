from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.status").isin(["COMPLETED", "RETURNED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
    )
    .orderBy(F.col("status_group").desc(), F.col("avg_amount").desc())
)
