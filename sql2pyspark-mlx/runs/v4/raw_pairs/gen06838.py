from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("rating") > 2.4)
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

agg = (
    recent_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
)

result = agg
