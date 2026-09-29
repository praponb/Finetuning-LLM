from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.region") == "East")
    .select("src.*", F.col("d.region"))
)
