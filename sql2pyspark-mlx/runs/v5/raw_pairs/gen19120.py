from pyspark.sql import functions as F

result = (
    df_suppliers
    .cube(
        "city",
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .orderBy(F.col("city_values").desc())
)
