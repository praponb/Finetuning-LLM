from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("ends_on", "id")
w4 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("channel").orderBy(F.col("cumulative_event_seq_2").desc_nulls_last(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-04-14").cast("date")) & (F.col("ends_on") <= "2024-10-01"))
    .select(
        "id",
        "channel",
        "ends_on",
        F.when(F.col("promo_code").isin(["WELCOME5", "SPRING10"]), "Tier 1").otherwise("Other").alias("promo_code_group"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped
    .select(
        "*",
        F.first("event_seq_2", ignorenulls=True).over(w4).alias("first_known_event_seq_2"),
        F.last("event_seq", ignorenulls=True).over(w5).alias("last_known_event_seq"),
        F.first("event_seq", ignorenulls=True).over(w6).alias("first_known_event_seq"),
        F.sum("event_seq_2").over(w7).alias("cumulative_event_seq_2"),
        F.first("channel", ignorenulls=True).over(w8).alias("first_known_channel"),
    )
)

ranked_promotions = (
    enriched
    .withColumn("dedup_rank_2", F.row_number().over(w9))
    .filter(F.col("dedup_rank_2") == 1)
)

valid = (
    ranked_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-31").cast("date")) & F.col("channel").isNotNull())
    .select(
        "*",
        (F.col("dedup_rank") * F.col("last_known_event_seq")).alias("dedup_rank_times_last_known_event_seq"),
    )
)

grouped = (
    valid
    .cube(
        F.when(F.col("event_seq") >= 9, "LARGE").otherwise("SMALL").alias("event_seq_tier"),
        F.when(F.col("first_known_event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("first_known_event_seq_tier"),
        "channel",
    )
    .agg(
        F.percentile_approx("event_seq_2", 0.95).alias("event_seq_2_p95"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("first_known_event_seq_2"), 2).alias("avg_first_known_event_seq_2"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
)

result = (
    grouped
    .select(
        "event_seq_tier",
        "first_known_event_seq_tier",
        "channel",
        "event_seq_2_p95",
        "avg_first_known_event_seq_2",
        "partner_rows",
    )
    .orderBy("partner_rows")
)
