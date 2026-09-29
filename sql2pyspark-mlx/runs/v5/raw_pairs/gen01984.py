from pyspark.sql import functions as F

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.delivered_date") >= "2023-08-01"), "left_anti")
)

result = (
    not_flagged
    .select("id", "store_id", "order_date", "status", "channel", "discount")
    .orderBy("store_id")
)
