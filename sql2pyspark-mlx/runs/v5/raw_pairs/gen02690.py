from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("o.channel") == "store")
    .groupBy(
        F.col("s.region"),
        F.date_trunc("MONTH", F.col("s.opened_date")).alias("opened_month"),
    )
    .agg(F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"))
    .orderBy("opened_month")
)
