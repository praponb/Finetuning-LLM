from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc())
w2 = Window.orderBy("start_date", "id")
w3 = Window.orderBy(F.col("start_date").desc(), "id")
w4 = Window.orderBy(F.col("event_seq").asc_nulls_first())
w5 = Window.orderBy("seq_no_2", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy(F.col("row_seq").desc(), "id")
w7 = Window.orderBy(F.col("event_seq_percent_rank").desc(), "id")
w8 = Window.orderBy("event_seq_avg_3", "id")
w9 = Window.partitionBy("seq_no").orderBy(F.col("row_seq_p90_2").desc())

filtered = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-07-26")))
    .select(
        "id",
        "start_date",
        "status",
        F.when(F.col("status").isin(["ACTIVE", "CANCELLED"]), "Group A").otherwise("Other").alias("status_group"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "start_date")
)

enriched = (
    deduped
    .filter(F.col("start_date") <= "2023-11-01")
    .select(
        "id",
        "start_date",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "start_date",
        "seq_no",
        "seq_no_2",
        F.percent_rank().over(w4).alias("event_seq_percent_rank"),
        F.avg("event_seq").over(w5).alias("event_seq_avg_3"),
        F.row_number().over(w6).alias("seq_no_3"),
    )
)

enriched_subscriptions = (
    with_windows
    .select(
        "id",
        "seq_no",
        "seq_no_2",
        "event_seq_avg_3",
        "seq_no_3",
        F.row_number().over(w7).alias("row_seq"),
        F.ntile(5).over(w8).alias("event_seq_avg_3_bucket"),
    )
)

result = (
    enriched_subscriptions
    .groupBy(
        "seq_no",
        F.when(F.col("seq_no_3") >= 7, "PREMIUM").otherwise("STANDARD").alias("seq_no_3_tier"),
        F.when(F.col("event_seq_avg_3_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_avg_3_bucket_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles_2"),
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90_2"),
        F.percentile_approx("event_seq_avg_3_bucket", 0.5).alias("event_seq_avg_3_bucket_p50"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles_3"),
    )
    .filter(F.col("event_seq_avg_3_bucket_p50") > 0)
    .withColumn("row_seq_p90_2_dense_rank", F.dense_rank().over(w9))
    .orderBy(F.col("event_seq_avg_3_bucket_p50").desc_nulls_last())
)
