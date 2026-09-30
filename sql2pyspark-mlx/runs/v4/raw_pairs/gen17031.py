from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.channel").isin(["store", "mobile"]))
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.countDistinct(F.col("o.amount")).alias("unique_channels"),
        F.sort_array(F.collect_set(F.col("o.status"))).alias("status_list"),
    )
    .filter(F.col("unique_channels") >= 2)
    .orderBy("opened_month")
    .limit(20)
)
