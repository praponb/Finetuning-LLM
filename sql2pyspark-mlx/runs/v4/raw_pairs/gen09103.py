from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("rating").between(2.6, 3.3) & F.col("rating").isNull())
    .withColumn("city_country", F.concat("city", F.lit("/"), "country"))
)

summary = (
    filtered_suppliers
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.count("rating").alias("rating_count"),
    )
)

result = (
    summary
    .filter(F.col("rating_count") < 8)
    .select("country", "pct_us")
    .orderBy(F.col("pct_us").desc())
)
