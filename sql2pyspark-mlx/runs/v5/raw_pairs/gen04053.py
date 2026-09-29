from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "country",
        "city",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy("min_rating")
)
