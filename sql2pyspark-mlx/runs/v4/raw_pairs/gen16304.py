from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "outer")
    .filter(~F.col("s.region").isin(["West", "North"]))
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("p.status")))).alias("status_values"),
        F.sum(F.col("p.amount")).alias("total_amount"),
    )
    .filter(F.col("total_amount") >= 3700)
)
