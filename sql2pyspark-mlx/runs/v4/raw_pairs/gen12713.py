from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.sqft"), F.col("d.store_name"))
)
