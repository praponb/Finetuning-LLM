from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_int", "id")
w3 = Window.partitionBy("channel").orderBy("ends_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("discount_pct_int_whole", "id")
w5 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-06-11"))) & (F.col("ends_on") > "2024-09-19"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

filtered = (
    cleaned
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-04-02"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.upper("promo_code").alias("promo_code_upper"),
        F.ceil("discount_pct_int").alias("discount_pct_int_whole"),
    )
)

with_windows = (
    filtered
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w))
    .withColumn("next_discount_pct_int", F.lead("discount_pct_int", 1).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

metrics = (
    with_windows
    .select(
        "*",
        F.lead("seq_no").over(w4).alias("next_seq_no"),
        F.lag("discount_pct_int_whole").over(w4).alias("prev_discount_pct_int_whole"),
        F.lead("discount_pct_int_whole").over(w4).alias("next_discount_pct_int_whole"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

attributed = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .withColumn("prev_discount_pct_int_whole_tier", F.when(F.col("prev_discount_pct_int_whole") >= 69, "LARGE").otherwise("SMALL"))
    .withColumn("activity_month", F.date_trunc("month", "starts_on"))
    .groupingSets(
        [["prev_discount_pct_int_whole_tier", "activity_month"], ["prev_discount_pct_int_whole_tier"]],
        "prev_discount_pct_int_whole_tier",
        "activity_month",
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.sum(F.when(F.col("channel") == "web", F.col("promotion_discount_pct"))).alias("web_total_promotion_discount_pct"),
    )
    .filter(F.col("avg_event_seq") > 0)
    .orderBy(F.col("activity_month").desc())
)
