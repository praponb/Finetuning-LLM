from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("sqft") != 13851) & (F.col("city") == "London")))
    .groupBy(
        F.when(F.col("sqft") >= 14367, "high").when(F.col("sqft") >= 6976, "mid").otherwise("low").alias("sqft_bucket"),
        "region",
    )
    .agg(F.countDistinct("sqft").alias("unique_regions"))
    .filter(F.col("unique_regions") > 2)
    .orderBy(F.col("unique_regions").desc(), "sqft_bucket", "region")
    .limit(10)
)
