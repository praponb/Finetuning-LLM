from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("signup_date").desc(), "id")
w2 = Window.partitionBy("city", "segment").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy("customer_id", "segment").orderBy(F.col("signup_date").desc(), "id")
w4 = Window.partitionBy("city").orderBy("signup_date", "id")
w5 = Window.partitionBy("segment").orderBy("seq_no_2", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("segment")
w_recent = Window.partitionBy("city").orderBy(F.col("city_stddev_samp_event_seq").asc_nulls_first())
w9 = Window.partitionBy("city", "segment")
w10 = Window.partitionBy("city", "segment").orderBy("city_stddev_samp_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

enriched = (
    eligible
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .filter(~F.col("b.segment").isin(["Corporate", "SMB"]))
    .select(
        "a.id",
        "a.customer_id",
        F.col("b.city"),
        F.col("b.signup_date"),
        F.col("b.segment"),
    )
)

with_windows = (
    enriched
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

with_windows_sessions = (
    with_windows
    .select(
        "id",
        "city",
        "signup_date",
        "segment",
        "row_seq",
        F.avg("row_seq").over(w5).alias("row_seq_avg_5"),
        F.stddev_samp("event_seq").over(w6).alias("city_stddev_samp_event_seq"),
    )
)

metrics = (
    with_windows_sessions
    .select(
        "id",
        "city",
        "signup_date",
        "row_seq_avg_5",
        "city_stddev_samp_event_seq",
        F.round((F.col("row_seq") / F.sum("row_seq").over(w7)), 4).alias("row_seq_share"),
        F.cume_dist().over(w_recent).alias("city_stddev_samp_event_seq_cume_dist"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w9)), 4).alias("row_seq_share_2"),
        F.sum("city_stddev_samp_event_seq").over(w10).alias("cumulative_city_stddev_samp_event_seq"),
    )
)

grouped = (
    metrics
    .groupBy(
        "city",
        F.when(F.col("city_stddev_samp_event_seq_cume_dist") >= 0.82, "HIGH").otherwise("LOW").alias("city_stddev_samp_event_seq_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("city_stddev_samp_event_seq_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("city_stddev_samp_event_seq_cume_dist_per_chiang_mai"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.avg("row_seq_share").alias("avg_row_seq_share"),
        F.min("city_stddev_samp_event_seq").alias("min_city_stddev_samp_event_seq"),
        F.percentile_approx("city_stddev_samp_event_seq", 0.9).alias("city_stddev_samp_event_seq_p90"),
    )
)

result = (
    grouped
    .select(
        "city",
        "city_stddev_samp_event_seq_cume_dist_tier",
        "bangkok_events",
        "min_city_stddev_samp_event_seq",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("bangkok_events").desc(), F.col("city_zone").desc())
)
