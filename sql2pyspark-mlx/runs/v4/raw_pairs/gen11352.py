from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft_whole", "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft_whole", "id")
w3 = Window.orderBy("sqft_whole", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("next_sqft").desc_nulls_first(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("next_sqft").desc_nulls_last(), "id")
w6 = Window.partitionBy("city").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("region", "city").orderBy(F.col("next_sqft").desc(), "id")
w8 = Window.partitionBy("city").orderBy(F.col("next_sqft").desc_nulls_last())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "East"))
    .select(
        "*",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.floor("sqft").alias("sqft_whole"),
    )
)

scored = (
    base
    .withColumn("next_sqft", F.lead("sqft", 1).over(w))
    .withColumn("sqft_whole_bucket", F.ntile(5).over(w2))
    .withColumn("sqft_whole_avg_6", F.avg("sqft_whole").over(w3))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

deduped_stores = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped_stores
    .select(
        "*",
        F.first("sqft_whole", ignorenulls=True).over(w6).alias("first_known_sqft_whole"),
        F.row_number().over(w7).alias("event_seq"),
        F.cume_dist().over(w8).alias("next_sqft_cume_dist"),
    )
)

matched_keys = (
    with_windows
    .select("id", "city")
    .exceptAll(
        df_stores
        .filter(F.month("opened_date").isin([1, 3, 7]))
        .select("id", "city")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("opened_date") > F.to_date(F.lit("2024-03-09")))
    .select(
        "*",
        F.round("event_seq", 0).alias("event_seq_rounded"),
        F.year("opened_date").alias("opened_date_year"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

summary = (
    filtered
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"), "city")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("region") == "East", F.col("next_sqft")).otherwise(0)).alias("east_next_sqft"),
        F.count("*").alias("cnt"),
        F.min("sqft_whole").alias("min_sqft_whole"),
        F.count_if(F.col("region") == "North").alias("north_events"),
    )
    .filter(F.col("min_sqft_whole") > 0)
)

result = (
    summary
    .select(
        "activity_month",
        "city",
        "row_count",
        "east_next_sqft",
        "cnt",
        "min_sqft_whole",
        "north_events",
    )
    .orderBy(F.col("cnt").desc())
)
