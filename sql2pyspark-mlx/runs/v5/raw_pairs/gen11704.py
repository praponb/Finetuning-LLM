from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("starts_on", "id")
w3 = Window.orderBy(F.col("ends_on_weekofyear").desc(), "id")
w4 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("seq_no", "id")
w_grp = Window.orderBy("ends_on_weekofyear", "id")
w_part = Window.orderBy("cumulative_starts_on_dayofweek", "id")

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.weekofyear("ends_on").alias("ends_on_weekofyear"),
        F.dayofweek("starts_on").alias("starts_on_dayofweek"),
    )
)

scored = (
    filtered
    .filter(F.col("ends_on") < "2024-03-17")
    .select(
        "*",
        F.sum("starts_on_dayofweek").over(w).alias("cumulative_starts_on_dayofweek"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("ends_on_weekofyear_bucket"),
        F.last("ends_on_weekofyear", ignorenulls=True).over(w4).alias("last_known_ends_on_weekofyear"),
    )
)

enriched = (
    scored
    .withColumn("prev_ends_on_weekofyear", F.lag("ends_on_weekofyear", 1).over(w5))
    .withColumn("ends_on_weekofyear_bucket_2", F.ntile(3).over(w_grp))
)

base = (
    enriched
    .filter((F.col("ends_on") > F.lit("2023-09-12").cast("date")) & (F.col("starts_on_dayofweek") <= 28))
    .select(
        "*",
        F.coalesce("ends_on_weekofyear_bucket", F.lit(0)).alias("ends_on_weekofyear_bucket_filled"),
        F.col("starts_on_dayofweek").cast("double").alias("starts_on_dayofweek_num"),
    )
)

valid = (
    base
    .filter(F.col("ends_on") > F.to_date(F.lit("2023-08-18")))
    .withColumn("ends_on_weekofyear_num", F.col("ends_on_weekofyear").cast("string"))
    .withColumn("starts_on_month_end", F.last_day("starts_on"))
)

metrics = (
    valid
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_dayofweek",
        "ends_on_weekofyear_bucket",
        "last_known_ends_on_weekofyear",
        "prev_ends_on_weekofyear",
        "ends_on_weekofyear_bucket_2",
        "ends_on_weekofyear_bucket_filled",
        "starts_on_dayofweek_num",
        F.lead("cumulative_starts_on_dayofweek").over(w_part).alias("next_cumulative_starts_on_dayofweek"),
        F.lead("ends_on_weekofyear_bucket_2").over(w_part).alias("next_ends_on_weekofyear_bucket_2"),
        F.lag("ends_on_weekofyear_bucket_2").over(w_part).alias("prev_ends_on_weekofyear_bucket_2"),
        F.row_number().over(w_part).alias("seq_no_2"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("ends_on_weekofyear_bucket_filled") >= 79, "PREMIUM").otherwise("STANDARD").alias("ends_on_weekofyear_bucket_filled_tier"),
        F.when(F.col("ends_on_weekofyear_bucket_2") >= 8, "PREMIUM").otherwise("STANDARD").alias("ends_on_weekofyear_bucket_2_tier"),
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("ends_on_weekofyear_bucket", 0.95).alias("ends_on_weekofyear_bucket_p95"),
        F.count("*").alias("row_count_2"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .select(
        "ends_on_weekofyear_bucket_filled_tier",
        "ends_on_weekofyear_bucket_2_tier",
        "year_start",
        "row_count",
        "ends_on_weekofyear_bucket_p95",
        "row_count_2",
    )
    .orderBy("row_count_2")
)
