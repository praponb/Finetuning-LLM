from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 1.9)
    .select("city", F.lit("supplier").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .distinct()
    .orderBy("city")
)
