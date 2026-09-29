from pyspark.sql import functions as F

result = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isNotNull()), "left_anti")
)
