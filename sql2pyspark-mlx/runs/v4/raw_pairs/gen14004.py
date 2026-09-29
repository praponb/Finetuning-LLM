from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(F.col("channel").isin(["web", "store"])).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_semi")
    .filter(F.col("stores.opened_date") < "2023-04-01")
    .select(F.col("stores.id"), F.col("stores.opened_date"))
)
