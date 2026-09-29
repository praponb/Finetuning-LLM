from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.upper(F.col("stores.store_name")).like("%IVE%"))
    .groupBy(
        F.col("orders.status"),
        F.date_trunc("MONTH", F.col("orders.order_date")).alias("order_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("payments.status")))).alias("status_values"),
        F.round(((100 * F.sum(F.when(F.col("payments.method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
)
