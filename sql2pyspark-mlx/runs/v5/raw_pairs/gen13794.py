from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("monthly_fee_rounded").asc_nulls_first(), "id")
w2 = Window.orderBy(F.col("start_date").desc(), "id")
w3 = Window.orderBy("start_date", "id")
w4 = Window.orderBy("event_seq", "id")

base = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-05-06").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "start_date")
)

scored = latest.withColumn("event_seq", F.row_number().over(w2))

enriched = (
    scored
    .filter(F.month("start_date").isin([3, 4, 11]))
    .select(
        "id",
        "start_date",
        F.lead("event_seq", 2).over(w3).alias("next_event_seq"),
        F.lead("event_seq", 1).over(w4).alias("next_event_seq_2"),
    )
)

filtered = (
    enriched
    .filter(F.col("start_date") >= F.lit("2023-02-23").cast("date"))
    .withColumn("days_since_start_date", F.datediff(F.current_date(), "start_date"))
)

cleaned = (
    filtered
    .filter(F.col("next_event_seq") > 3)
    .select(
        "id",
        "start_date",
        "next_event_seq",
        "days_since_start_date",
        F.greatest("next_event_seq_2", "days_since_start_date").alias("greatest_next_event_seq_2_days_since_start_date"),
    )
)

daily = (
    cleaned
    .groupBy(
        F.when(F.col("days_since_start_date") >= 90, "HIGH").otherwise("LOW").alias("days_since_start_date_tier"),
        F.when(F.col("greatest_next_event_seq_2_days_since_start_date") >= 82, "PREMIUM").otherwise("STANDARD").alias("greatest_next_event_seq_2_days_since_start_date_tier"),
        F.date_trunc("week", "start_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("greatest_next_event_seq_2_days_since_start_date"), 2).alias("avg_greatest_next_event_seq_2_days_since_start_date"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.avg("next_event_seq").alias("avg_next_event_seq"),
    )
)

result = (
    daily
    .select(
        "days_since_start_date_tier",
        "greatest_next_event_seq_2_days_since_start_date_tier",
        "week_start",
        "avg_greatest_next_event_seq_2_days_since_start_date",
        "distinct_ids_2",
        "avg_next_event_seq",
    )
    .orderBy("greatest_next_event_seq_2_days_since_start_date_tier")
)
