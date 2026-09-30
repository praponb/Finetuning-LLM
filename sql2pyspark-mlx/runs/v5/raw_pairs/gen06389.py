from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec_adjusted_minus_event_time_month").desc_nulls_first(), "id")
w_recent = Window.partitionBy("page").orderBy("duration_sec_adjusted_minus_event_time_month", "id")
w3 = Window.partitionBy("page").orderBy("seq_no", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("page").orderBy(F.col("prev_duration_sec_adjusted_minus_event_time_month").desc(), "id")
w5 = Window.orderBy("next_duration_sec_adjusted_minus_event_time_month", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("page").orderBy("duration_sec_adjusted_minus_event_time_month", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("prev_duration_sec_adjusted_minus_event_time_month", "id")

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-07-23").cast("date")) & F.col("duration_sec").between(218, 257))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_adjusted"),
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
    )
)

base = (
    cleaned
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_adjusted",
        "page_event_type",
        F.month("event_time").alias("event_time_month"),
        F.trim("page").alias("clean_page"),
    )
)

filtered = (
    base
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_adjusted_minus_event_time_month", (F.col("duration_sec_adjusted") - F.col("event_time_month")))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("double"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("DIRECT")))
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "page_event_type",
        "duration_sec_adjusted_minus_event_time_month",
    )
)

scored = (
    ranked
    .select(
        "id",
        "page",
        "duration_sec",
        "page_event_type",
        "duration_sec_adjusted_minus_event_time_month",
        F.lag("duration_sec").over(w_recent).alias("prev_duration_sec"),
        F.lag("duration_sec_adjusted_minus_event_time_month").over(w_recent).alias("prev_duration_sec_adjusted_minus_event_time_month"),
        F.lead("duration_sec_adjusted_minus_event_time_month").over(w_recent).alias("next_duration_sec_adjusted_minus_event_time_month"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

enriched = (
    scored
    .withColumn("duration_sec_adjusted_minus_event_time_month_avg_5_prior", F.avg("duration_sec_adjusted_minus_event_time_month").over(w3))
    .withColumn("seq_no_2", F.row_number().over(w_ordered))
)

scored_web_events = (
    enriched
    .filter(F.col("prev_duration_sec") > 185)
    .select(
        "id",
        "page",
        "page_event_type",
        "duration_sec_adjusted_minus_event_time_month",
        "prev_duration_sec",
        "prev_duration_sec_adjusted_minus_event_time_month",
        F.avg("duration_sec_adjusted_minus_event_time_month").over(w5).alias("duration_sec_adjusted_minus_event_time_month_avg_5_prior_2"),
        F.avg("prev_duration_sec_adjusted_minus_event_time_month").over(w6).alias("prev_duration_sec_adjusted_minus_event_time_month_avg_3_prior"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

result = (
    scored_web_events
    .select(
        "id",
        "page",
        "prev_duration_sec_adjusted_minus_event_time_month",
        "duration_sec_adjusted_minus_event_time_month",
    )
    .unpivot(
        ids=["id", "page"],
        values=["prev_duration_sec_adjusted_minus_event_time_month", "duration_sec_adjusted_minus_event_time_month"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
