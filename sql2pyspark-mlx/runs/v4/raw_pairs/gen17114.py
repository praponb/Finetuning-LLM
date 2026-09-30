from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") > 4)
    .select("rating_bucket", "city", "country_values", "unique_countrys")
)
