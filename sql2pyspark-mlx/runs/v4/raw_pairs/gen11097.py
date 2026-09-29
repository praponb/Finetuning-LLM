from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("rating")).otherwise(0)).alias("london_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") >= 2)
    .drop("unique_countrys")
    .orderBy("city", F.col("jp_count").desc())
)
