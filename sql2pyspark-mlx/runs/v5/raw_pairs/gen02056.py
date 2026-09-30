from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("started_at", "id")
w_grp = Window.partitionBy("device").orderBy("burst_id", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("last3_min_seq_no").desc(), "id")
w8 = Window.partitionBy("country").orderBy(F.col("last3_max_seq_no").asc_nulls_last(), "id")

metrics = (
    df_sessions
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.first("country", ignorenulls=True).over(w_recent).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w2).alias("first_known_browser"),
        F.first("browser", ignorenulls=True).over(w_ordered).alias("first_known_browser_2"),
        F.last("country", ignorenulls=True).over(w4).alias("last_known_country"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w5).isNull() | (F.col("started_at") > F.lag("started_at").over(w5) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "first_known_browser_2",
        "last_known_country",
        "seq_no",
        F.sum("session_start_flag").over(w_recent).alias("burst_id"),
    )
)

metrics_sessions = (
    sessionized
    .select(
        "*",
        F.max("seq_no").over(w_grp).alias("last3_max_seq_no"),
        F.avg("seq_no").over(w_grp).alias("last3_avg_seq_no"),
        F.min("seq_no").over(w_grp).alias("last3_min_seq_no"),
    )
)

latest = (
    metrics_sessions
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device",
        "country",
        "first_known_browser_2",
        "last_known_country",
        "last3_max_seq_no",
        "last3_avg_seq_no",
        "last3_min_seq_no",
    )
)

latest_sessions = (
    latest
    .withColumn("latest_rank", F.row_number().over(w8))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest_sessions
    .cube("device", "country")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.grouping("device").alias("grouping_id"),
    )
    .filter(F.col("tablet_events") > 1)
    .orderBy(F.col("device").desc())
)
