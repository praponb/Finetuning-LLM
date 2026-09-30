from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.store_name"))
)
