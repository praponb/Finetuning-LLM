from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.5) & (F.upper(F.trim("supplier_name")) == "SIAM PARTS CO"))
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
    )
    .orderBy("avg_rating")
)
