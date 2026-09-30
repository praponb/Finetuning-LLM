from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter((F.col("country") != "DE") & (F.col("country") == "JP"))
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit("/"), "country"))
)

summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)

result = (
    summary
    .filter(F.col("stddev_rating") < 21)
    .select("rating_bucket", "unique_countrys", "stddev_rating")
    .orderBy("rating_bucket")
)
