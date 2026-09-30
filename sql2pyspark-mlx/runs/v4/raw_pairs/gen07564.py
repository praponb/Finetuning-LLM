from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc(), "id")
w_running = Window.orderBy("rn", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc_nulls_last())
w7 = Window.partitionBy("page").orderBy(F.col("duration_sec_avg_2_prior").desc())
w8 = Window.partitionBy("event_type").orderBy("event_time", "id")
w9 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .withColumn("duration_sec_avg_30d", F.avg("duration_sec").over(w2))
    .withColumn("duration_sec_bucket", F.ntile(5).over(w3))
    .withColumn("duration_sec_avg_2_prior", F.avg("duration_sec").over(w_running))
    .withColumn("duration_sec_max_7d", F.max("duration_sec").over(w5))
    .withColumn("duration_sec_rank", F.rank().over(w6))
)

deduped_web_events = (
    scored
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

flagged = (
    deduped_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w8) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w9))

result = (
    sessionized
    .groupBy(
        F.when(F.col("duration_sec_avg_30d") >= 362, "LARGE").otherwise("SMALL").alias("duration_sec_avg_30d_tier"),
        F.when(F.col("duration_sec_avg_2_prior") >= 374, "LARGE").otherwise("SMALL").alias("duration_sec_avg_2_prior_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
        F.min("event_time").alias("first_event_time"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.percentile_approx("island_id", [0.25, 0.5, 0.75]).alias("island_id_quartiles"),
    )
    .filter(F.col("purchase_duration_sec") < 0)
)
