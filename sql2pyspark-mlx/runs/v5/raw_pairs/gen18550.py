from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "US"]))
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") < 12)
    .orderBy("rating_bucket")
)
