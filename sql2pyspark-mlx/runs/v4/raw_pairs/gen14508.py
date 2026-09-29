from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.opened_date"), F.col("s.city"))
)
