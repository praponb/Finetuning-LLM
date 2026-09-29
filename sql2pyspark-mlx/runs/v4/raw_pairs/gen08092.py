from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country") == "SG")

suppliers_stats = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
)

result = (
    suppliers_stats
    .select("country", "us_rating")
    .orderBy(F.col("us_rating").desc())
)
