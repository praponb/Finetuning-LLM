from pyspark.sql import functions as F

result = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isNull()), "left_anti")
)
