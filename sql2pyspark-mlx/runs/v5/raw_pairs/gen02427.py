from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") != "SG")

summary = (
    src
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    summary
    .select("city", "rating_bucket", "city_values")
    .orderBy("rating_bucket", "city")
    .limit(20)
)
