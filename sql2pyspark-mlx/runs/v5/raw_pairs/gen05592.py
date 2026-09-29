from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("is_page_view", "id").rowsBetween(-5, Window.currentRow)
w_grp = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("burst_id", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("session_id", "page").orderBy(F.col("burst_id").desc())

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([4, 9, 24, 37]))
    .select(
        "*",
        F.when(F.col("event_type") == "page_view", 1).otherwise(0).alias("is_page_view"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w3))

scored = (
    sessionized
    .filter(F.col("is_page_view") <= 78)
    .withColumn("burst_id_avg_5", F.avg("burst_id").over(w_part))
    .withColumn("is_page_view_avg_7d", F.avg("is_page_view").over(w_grp))
    .withColumn("duration_sec_avg_6", F.avg("duration_sec").over(w6))
    .withColumn("burst_id_percent_rank", F.percent_rank().over(w7))
)

stats = (
    scored
    .groupBy(
        F.when(F.col("duration_sec") >= 407, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
        "burst_id",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("is_page_view")).otherwise(0)).alias("purchase_is_page_view"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    stats
    .select("duration_sec_tier", "burst_id", "purchase_is_page_view", "page_list")
    .orderBy(F.col("duration_sec_tier").desc())
)
