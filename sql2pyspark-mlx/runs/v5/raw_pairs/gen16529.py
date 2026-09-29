from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating") > 2.7)

suppliers_stats = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("jp_rating") >= 28)
    .orderBy(F.col("country").desc())
)
