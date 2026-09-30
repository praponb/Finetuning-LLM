from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser", "device").orderBy("started_at", "id")
w_ordered = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("started_at", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("country")

with_windows = (
    df_sessions
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("first_known_browser_2", F.first("browser", ignorenulls=True).over(w_ordered))
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page") == "/home"), "left_anti")
)

with_windows_sessions = (
    kept
    .withColumn("event_seq_avg_3_prior", F.avg("event_seq").over(w4))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4))
)

result = (
    with_windows_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.sum(F.when(F.col("browser") == "Firefox", F.col("event_seq_avg_3_prior")).otherwise(0)).alias("firefox_event_seq_avg_3_prior"),
        F.min("started_at").alias("first_seen"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.sum(F.when(F.col("device") == "mobile", F.col("event_seq_avg_3_prior")).otherwise(0)).alias("mobile_event_seq_avg_3_prior"),
        F.max("started_at").alias("ended_at"),
    )
    .orderBy(F.col("event_seq_p95").desc_nulls_last())
)
