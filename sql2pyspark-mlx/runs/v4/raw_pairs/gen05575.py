from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("country").isin(["US", "JP", "TH"]) & (F.col("city") == "Berlin") & (F.col("city") != "London"))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
)

summary = (
    src
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
    )
)

result = (
    summary
    .filter(F.col("stddev_rating") >= 59)
    .select("country", "rating_bucket", "stddev_rating", "city_values")
)
