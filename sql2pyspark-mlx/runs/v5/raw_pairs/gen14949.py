from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("price") >= 1050))
    .withColumn("brand_prefix", F.substring("brand", 1, 3))
)

result = (
    valid
    .groupBy(
        F.date_trunc("month", "launch_date").alias("activity_month"),
        "supplier_id",
    )
    .agg(
        F.percentile_approx("cost", 0.5).alias("cost_p50"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .orderBy("cost_p50")
)
