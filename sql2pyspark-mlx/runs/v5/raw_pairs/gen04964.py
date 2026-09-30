from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
    .orderBy(F.col("pct_card").desc())
)
