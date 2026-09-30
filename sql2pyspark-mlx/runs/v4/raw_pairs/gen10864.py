from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.sqft"), F.col("d.opened_date"))
)
