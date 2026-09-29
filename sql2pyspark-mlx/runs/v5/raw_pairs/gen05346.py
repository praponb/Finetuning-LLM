from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
        F.col("s.city"),
    )
    .agg(
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
        F.sum(F.when(F.col("o.channel") == "web", F.col("o.amount")).otherwise(0)).alias("web_amount"),
    )
    .orderBy("city")
)
