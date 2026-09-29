from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id") == 12), "left_anti")
)

result = (
    kept
    .groupBy("channel", "store_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)
