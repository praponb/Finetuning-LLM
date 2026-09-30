from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.4) & (F.col("country") == "UK") & F.col("country").isin(["UK", "JP"]))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
        F.when(F.col("country").isin(["TH", "JP"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
    .orderBy(F.col("country_group").desc(), "rating_bucket", "city")
    .limit(3)
)
