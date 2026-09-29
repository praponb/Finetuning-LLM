from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
        F.sum(F.when(F.col("p.status") == "SUCCESS", F.col("p.amount")).otherwise(0)).alias("success_amount"),
    )
    .orderBy("channel")
    .limit(5)
)
