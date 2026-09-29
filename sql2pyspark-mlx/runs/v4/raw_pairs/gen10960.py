from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first(), "id")

deduped = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

cleaned = (
    deduped
    .filter(F.col("country") == "SG")
    .withColumn("city_filled", F.coalesce("city", F.lit("N/A")))
    .withColumn("country_city", F.concat("country", F.lit(" "), "city"))
    .withColumn("city_filled_2", F.coalesce("city", F.lit("N/A")))
)

filtered = (
    cleaned
    .filter(F.col("city").isNotNull() & (F.col("country") == "JP"))
    .withColumn("country_prefix", F.substring("country", 1, 3))
)

result = (
    filtered
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.5, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
    )
    .orderBy(F.col("rating_tier").desc())
)
