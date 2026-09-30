from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

summary = (
    base
    .groupBy("country", "city")
    .agg(
        F.max("is_singapore").alias("max_is_singapore"),
        F.avg("is_singapore").alias("avg_is_singapore"),
    )
)

result = (
    summary
    .filter(F.col("avg_is_singapore") > 62)
    .select("country", "city", "max_is_singapore", "avg_is_singapore")
    .orderBy(F.col("max_is_singapore").desc())
)
