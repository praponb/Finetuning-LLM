from pyspark.sql import functions as F

base = df_suppliers.filter(F.length("supplier_name") > 12)

summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
)

result = (
    summary
    .filter(F.col("de_rating") > 75)
    .select("rating_bucket", "country", "city_list", "de_rating")
)
