from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("status", "plan").orderBy(F.col("end_date").desc(), "id")
w3 = Window.partitionBy("plan", "status").orderBy(F.col("start_date").desc(), "id")
w4 = Window.partitionBy("status").orderBy(F.col("end_date").desc_nulls_last(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("seq_no").desc())
w_grp = Window.partitionBy("customer_id", "status").orderBy("event_seq_2")
w_part = Window.orderBy(F.col("event_seq").desc_nulls_last())
w_recent = Window.partitionBy("plan").orderBy("seq_no")
w9 = Window.partitionBy("customer_id")
w10 = Window.orderBy(F.col("event_seq_percent_rank").desc_nulls_last())
w11 = Window.partitionBy("plan")
w12 = Window.partitionBy("status")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2024-04-12").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.upper("plan").alias("plan_upper"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    latest
    .select("id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([18, 27]))
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "CANCELLED"))
    .withColumn("plan_lower", F.lower("plan"))
)

enriched = (
    base
    .withColumn("seq_no_percent_rank", F.percent_rank().over(w5))
    .withColumn("event_seq_2_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("event_seq_percent_rank", F.percent_rank().over(w_part))
)

matched_keys_2 = (
    enriched
    .select("plan")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") >= "2023-03-01")
        .select("plan")
    )
)

verified_subscriptions = (
    enriched
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified_subscriptions
    .withColumn("seq_no_percent_rank_2", F.percent_rank().over(w_recent))
    .withColumn("customer_id_sum_event_seq_2_cume_dist", F.sum("event_seq_2_cume_dist").over(w9))
    .withColumn("event_seq_percent_rank_cume_dist", F.cume_dist().over(w10))
    .withColumn("seq_no_share", F.round((F.col("seq_no") / F.sum("seq_no").over(w11)), 4))
    .withColumn("seq_no_share_2", F.round((F.col("seq_no") / F.sum("seq_no").over(w12)), 4))
)
