from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "US", "UK"]))
    .withColumn("country_group", F.when(F.col("country").isin(["UK", "US"]), "Core").otherwise("Other"))
)

rating_bucket_summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "SG", F.col("rating")).otherwise(0)).alias("sg_rating"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("sg_rating") < 27)
    .select("rating_bucket", "sg_rating")
)
