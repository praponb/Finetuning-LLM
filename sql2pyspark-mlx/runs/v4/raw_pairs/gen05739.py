from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(~F.col("country").isin(["US", "SG", "DE"]) | F.lower("supplier_name").like("%aci%"))
)

agg = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
)

result = agg.select("rating_bucket", "country", "max_rating", "singapore_count")
