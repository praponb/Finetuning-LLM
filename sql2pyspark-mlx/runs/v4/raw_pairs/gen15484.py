from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.when(F.col("o.amount") >= 1390, "high").when(F.col("o.amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.method")))).alias("method_values"),
        F.count(F.when(F.col("p.method") == "card", 1)).alias("card_count"),
    )
    .orderBy("method_values")
)
