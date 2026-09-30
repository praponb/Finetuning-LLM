from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.orderBy("rating", "id")

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug_2"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "city",
        "rating",
        F.last("rating", ignorenulls=True).over(w).alias("last_known_rating"),
        F.ntile(5).over(w2).alias("rating_bucket"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") >= "2025-04-14"), "left_semi")
)

enriched = (
    eligible
    .select(
        "*",
        F.lag("rating").over(w3).alias("prev_rating"),
        F.lead("rating_bucket").over(w3).alias("next_rating_bucket"),
        F.row_number().over(w3).alias("row_seq"),
        F.lead("last_known_rating", 1).over(w4).alias("next_last_known_rating"),
    )
)

grouped = (
    enriched
    .groupBy(
        "city",
        F.when(F.col("next_last_known_rating") >= 4.5, "HIGH").otherwise("LOW").alias("next_last_known_rating_tier"),
    )
    .agg(
        F.max("rating_bucket").alias("max_rating_bucket"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.avg("rating_bucket").alias("avg_rating_bucket"),
    )
)

result = (
    grouped
    .select(
        "city",
        "next_last_known_rating_tier",
        "max_rating_bucket",
        "avg_row_seq",
        "event_count",
        "row_seq_p50",
        "avg_rating_bucket",
    )
    .orderBy(F.col("avg_row_seq").asc_nulls_first(), "row_seq_p50")
)
