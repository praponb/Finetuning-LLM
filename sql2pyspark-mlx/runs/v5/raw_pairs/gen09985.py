from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.opened_date") <= "2024-11-01")
    .select(F.col("s.id"), F.col("s.store_name"))
    .orderBy(F.col("id").desc(), F.col("store_name").desc())
)
