from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy(F.col("country").desc())
)
