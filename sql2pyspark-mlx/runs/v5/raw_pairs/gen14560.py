from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w_ordered = Window.partitionBy("page").orderBy("event_time_weekofyear", "id")
w3 = Window.partitionBy("page").orderBy(F.col("last7_avg_event_time_weekofyear").desc_nulls_first(), "id")
w4 = Window.partitionBy("page").orderBy("last7_sum_event_time_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("prev_event_time_weekofyear", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("page", "event_type")
w8 = Window.partitionBy("page").orderBy(F.col("last7_count_event_time_weekofyear").desc_nulls_first())
w9 = Window.partitionBy("page").orderBy("last7_count_event_time_weekofyear_rank", "id")

base = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-03-25").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.weekofyear("event_time").alias("event_time_weekofyear"),
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

scored = (
    base
    .filter(F.to_date("event_time") >= "2024-10-01")
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "event_time_weekofyear",
        "resolved_page",
        F.avg("event_time_weekofyear").over(w_recent).alias("last7_avg_event_time_weekofyear"),
        F.count("event_time_weekofyear").over(w_recent).alias("last7_count_event_time_weekofyear"),
        F.sum("event_time_weekofyear").over(w_recent).alias("last7_sum_event_time_weekofyear"),
        F.max("event_time_weekofyear").over(w_recent).alias("last7_max_event_time_weekofyear"),
        F.lag("event_time_weekofyear", 1).over(w_ordered).alias("prev_event_time_weekofyear"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored_web_events = (
    deduped
    .select(
        "id",
        "page",
        "event_time",
        "event_time_weekofyear",
        "last7_sum_event_time_weekofyear",
        "last7_max_event_time_weekofyear",
        "prev_event_time_weekofyear",
        F.last("last7_max_event_time_weekofyear", ignorenulls=True).over(w4).alias("last_known_last7_max_event_time_weekofyear"),
        F.sum("last7_avg_event_time_weekofyear").over(w5).alias("running_last7_avg_event_time_weekofyear"),
        F.first("last7_sum_event_time_weekofyear", ignorenulls=True).over(w6).alias("first_known_last7_sum_event_time_weekofyear"),
        F.sum("last7_sum_event_time_weekofyear").over(w7).alias("page_sum_last7_sum_event_time_weekofyear"),
        F.rank().over(w8).alias("last7_count_event_time_weekofyear_rank"),
    )
)

scored_web_events_2 = (
    scored_web_events
    .withColumn("last7_count_event_time_weekofyear_rank_bucket", F.ntile(3).over(w9))
    .withColumn("prev_last7_count_event_time_weekofyear_rank", F.lag("last7_count_event_time_weekofyear_rank", 1, 0).over(w_ordered))
)

daily = (
    scored_web_events_2
    .groupBy("page")
    .agg(
        F.round(F.sum("event_time_weekofyear") / F.nullif(F.count(F.when(F.col("page") == "/checkout", 1)), F.lit(0)), 2).alias("event_time_weekofyear_per_checkout"),
        F.avg("last7_count_event_time_weekofyear_rank").alias("avg_last7_count_event_time_weekofyear_rank"),
        F.sum(F.when(F.col("page") == "/cart", F.col("event_time_weekofyear")).otherwise(0)).alias("cart_event_time_weekofyear"),
        F.round(F.sum("last7_max_event_time_weekofyear") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("last7_max_event_time_weekofyear_per_search"),
        F.countDistinct("page").alias("distinct_pages"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .filter(F.col("distinct_pages") >= 1)
)

result = (
    daily
    .select(
        "page",
        "event_time_weekofyear_per_checkout",
        "cart_event_time_weekofyear",
        "last7_max_event_time_weekofyear_per_search",
        "distinct_pages",
        "checkout_count",
    )
)
