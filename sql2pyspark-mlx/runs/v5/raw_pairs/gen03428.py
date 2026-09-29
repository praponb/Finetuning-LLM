from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
    .filter(F.col("pct_jp") > 58)
)
