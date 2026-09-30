from pyspark.sql import functions as F

result = (
    df_customers
    .select("city", F.lit("customer").alias("source"))
    .unionByName(df_stores.select("city", F.lit("store").alias("source")))
    .distinct()
    .orderBy(F.col("city").desc())
)
