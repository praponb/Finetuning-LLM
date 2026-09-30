from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("running_sum_rating_tier").orderBy(F.col("uk_total_cumulative_rating").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Berlin", "Singapore"]))
    .select("*", F.initcap(F.trim("city")).alias("city_title"))
)

metrics = (
    filtered
    .select(
        "*",
        F.sum("rating").over(w).alias("running_sum_rating"),
        F.count("rating").over(w).alias("running_count_rating"),
        F.min("rating").over(w).alias("running_min_rating"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
    )
)

cleaned = (
    metrics
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "rating",
        "city_title",
        "running_sum_rating",
        "running_min_rating",
        "cumulative_rating",
        F.concat("supplier_name", F.lit(" - "), "city").alias("supplier_name_city"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.running_sum_rating",
        "s.running_min_rating",
        "s.cumulative_rating",
        "s.supplier_name_city",
        F.col("c.launch_date"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    with_last
    .groupBy(
        F.when(F.col("running_sum_rating") >= 15.6, "PREMIUM").otherwise("STANDARD").alias("running_sum_rating_tier"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("cumulative_rating"))).alias("uk_total_cumulative_rating"),
        F.percentile_approx("running_sum_rating", 0.5).alias("running_sum_rating_p50"),
        F.count("*").alias("row_count"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .withColumn("uk_total_cumulative_rating_dense_rank", F.dense_rank().over(w4))
)
