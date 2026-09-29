from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date", F.lit("product").alias("source"))
    .union(df_stores.select("opened_date", F.lit("store").alias("source")))
    .distinct()
    .orderBy(F.col("source").desc())
)
