from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(F.lit("supplier").alias("kind"), "city")
    .unionByName(df_stores.select(F.lit("store").alias("kind"), "city"))
    .orderBy("kind")
)
