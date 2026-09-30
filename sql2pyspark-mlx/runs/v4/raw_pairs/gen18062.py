from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") >= 9) & (F.col("rating") >= 3.0))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
    .orderBy("rating_bucket")
)
