from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.region"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .orderBy("pct_refunded")
)
