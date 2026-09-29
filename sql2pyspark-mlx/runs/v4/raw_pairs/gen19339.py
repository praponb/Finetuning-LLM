from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.col("rating").cast("int").alias("rating_int"),
    )
)

result = (
    cleaned
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW"))
    .rollup("city", "rating_tier")
    .agg(
        F.percentile_approx("rating_int", [0.25, 0.5, 0.75]).alias("rating_int_quartiles"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("city") == "Singapore", F.col("rating_int"))).alias("singapore_max_rating_int"),
        F.grouping("city").alias("grouping_id"),
    )
)
