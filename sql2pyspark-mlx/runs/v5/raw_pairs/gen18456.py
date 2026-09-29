from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.count("*").alias("num_suppliers"),
    )
    .filter(F.col("new_york_count") >= 80)
    .select("rating_bucket", "new_york_count", "num_suppliers")
    .orderBy("rating_bucket")
    .limit(5)
)
