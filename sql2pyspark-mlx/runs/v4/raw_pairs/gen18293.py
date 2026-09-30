from pyspark.sql import functions as F

eligible = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.order_id").isin([13, 36, 38])), "left_anti")
)

result = eligible.select("id", "customer_id", "store_id", "channel", "amount", "discount")
