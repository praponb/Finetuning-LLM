from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating_bucket", "id")
w4 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city", "country").orderBy(F.col("row_seq").asc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["Tokyo", "London", "New York"]))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.trim("country").alias("clean_country"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

metrics = (
    base
    .select(
        "id",
        "country",
        "city",
        "clean_country",
        "resolved_city",
        F.ntile(4).over(w).alias("rating_bucket"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.lead("event_seq").over(w3).alias("next_event_seq"),
        F.lag("rating_bucket").over(w3).alias("prev_rating_bucket"),
        F.lead("rating_bucket").over(w3).alias("next_rating_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("rating_bucket").over(w4).alias("cumulative_rating_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

base_suppliers = (
    latest
    .filter(F.col("next_event_seq") >= 5)
    .select(
        "id",
        "country",
        "city",
        "clean_country",
        "resolved_city",
        "rating_bucket",
        "event_seq",
        "next_event_seq",
        "prev_rating_bucket",
        "next_rating_bucket",
        "row_seq",
        "cumulative_rating_bucket",
        "rn",
        F.round("next_rating_bucket", 0).alias("next_rating_bucket_rounded"),
        F.round((F.col("row_seq") / F.col("cumulative_rating_bucket")), 2).alias("row_seq_per_cumulative_rating_bucket"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city_2"),
    )
)

result = (
    base_suppliers
    .groupBy("country")
    .agg(
        F.percentile_approx("cumulative_rating_bucket", 0.95).alias("cumulative_rating_bucket_p95"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.percentile_approx("prev_rating_bucket", [0.25, 0.5, 0.75]).alias("prev_rating_bucket_quartiles"),
    )
    .orderBy(F.col("chiang_mai_count").desc())
)
