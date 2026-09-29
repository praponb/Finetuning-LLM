from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.opened_date") >= "2023-10-01")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.region"), F.col("s.opened_date"))
)
