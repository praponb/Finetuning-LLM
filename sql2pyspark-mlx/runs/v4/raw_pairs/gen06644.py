from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("end_date", "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("end_date", "id")
w4 = Window.partitionBy("status").orderBy(F.col("start_date_plus_16d").desc(), "id")
w5 = Window.orderBy(F.col("start_date_plus_16d").desc(), "id")
w6 = Window.partitionBy("status").orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("status", "customer_id")
w9 = Window.partitionBy("customer_id")
w10 = Window.partitionBy("customer_id").orderBy(F.col("start_date_plus_16d").asc_nulls_last(), "id")
w11 = Window.partitionBy("status")
w12 = Window.partitionBy("status").orderBy(F.col("event_seq_avg_3").desc())
w13 = Window.partitionBy("status").orderBy("event_seq")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-03-23").cast("date")) & ~F.col("plan").isin(["standard", "premium"]))
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.date_add("start_date", 16).alias("start_date_plus_16d"),
    )
)

scored = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
    .withColumn("row_seq_2", F.row_number().over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

metrics = (
    scored
    .withColumn("running_row_seq_2", F.sum("row_seq_2").over(w6))
    .withColumn("event_seq_avg_3", F.avg("event_seq").over(w7))
    .withColumn("status_sum_row_seq_2", F.sum("row_seq_2").over(w8))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w9)), 4))
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
)

matched_keys = (
    latest
    .select("id", "status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("plan") == "enterprise")
        .select("id", "status")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored_subscriptions = (
    kept_rows
    .withColumn("status_stddev_samp_row_seq", F.stddev_samp("row_seq").over(w11))
    .withColumn("event_seq_avg_3_percent_rank", F.percent_rank().over(w12))
    .withColumn("event_seq_cume_dist", F.cume_dist().over(w13))
)

result = (
    scored_subscriptions
    .withColumn("event_seq_share_tier", F.when(F.col("event_seq_share") >= 0.66, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date"))
    .cube("event_seq_share_tier", "quarter_start")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("num_status").desc())
)
