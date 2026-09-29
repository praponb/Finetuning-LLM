from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter((F.col("rating") >= 2.4) & F.col("supplier_name").like("%Lion%") & (F.col("city") == "Bangkok"))
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
)

summary = (
    filtered_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = (
    summary
    .select("country", "rating_bucket", "city_list")
    .orderBy("rating_bucket", "country")
)
