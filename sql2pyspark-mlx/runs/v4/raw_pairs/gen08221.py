from pyspark.sql import functions as F

result = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.order_date"), F.col("o.status"), F.col("o.channel"))
)
