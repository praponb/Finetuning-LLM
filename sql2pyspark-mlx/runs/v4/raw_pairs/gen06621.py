from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3]))
    .withColumn("revenue_int", F.col("revenue").cast("int"))
)

result = (
    base
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"), F.col("d.store_name"))
)
