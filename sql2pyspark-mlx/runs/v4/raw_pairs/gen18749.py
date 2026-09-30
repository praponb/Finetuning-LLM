from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "paypal", "cash"]))
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .filter(F.col("pbo.pct_paypal") > 65)
    .select(
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("pbo.method_list"),
        F.col("pbo.pct_paypal"),
    )
)
