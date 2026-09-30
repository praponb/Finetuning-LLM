from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w_grp = Window.partitionBy("plan").orderBy("start_date", "id")
w6 = Window.partitionBy("customer_id", "plan").orderBy(F.col("end_date").desc(), "id")
w7 = Window.orderBy(F.col("next_monthly_fee").desc(), "id")
w8 = Window.partitionBy("customer_id").orderBy("first_known_monthly_fee", "id")
w9 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_bucket_2").desc_nulls_first())
w10 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_bucket_2").desc_nulls_last())
w11 = Window.partitionBy("customer_id").orderBy("monthly_fee_bucket_2_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("customer_id").orderBy("next_monthly_fee", "id")
w13 = Window.partitionBy("customer_id")
w14 = Window.orderBy(F.col("monthly_fee_bucket_2_rank").asc_nulls_first())
w15 = Window.partitionBy("customer_id").orderBy("first_known_monthly_fee_bucket", "id")

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 70)
    .select("*", F.date_format("start_date", "yyyy-MM").alias("start_date_label"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "plan", "start_date", "end_date", "monthly_fee")
)

enriched = (
    deduped
    .filter(F.col("monthly_fee").between(90, 230))
    .select(
        "id",
        "customer_id",
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
        F.ntile(3).over(w4).alias("monthly_fee_bucket_2"),
        F.lead("monthly_fee", 1, 0).over(w_grp).alias("next_monthly_fee"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w7).alias("event_seq_2"),
        F.ntile(5).over(w8).alias("first_known_monthly_fee_bucket"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.rank().over(w9).alias("monthly_fee_bucket_2_rank"),
        F.percent_rank().over(w10).alias("monthly_fee_bucket_2_percent_rank"),
    )
)

scored = (
    with_windows
    .withColumn("first_known_next_monthly_fee", F.first("next_monthly_fee", ignorenulls=True).over(w11))
    .withColumn("row_seq", F.row_number().over(w12))
    .withColumn("customer_id_sum_event_seq_2", F.sum("event_seq_2").over(w13))
    .withColumn("monthly_fee_bucket_2_rank_percent_rank", F.percent_rank().over(w14))
    .withColumn("next_first_known_monthly_fee", F.lead("first_known_monthly_fee", 1).over(w15))
)

enriched_subscriptions = (
    scored
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.monthly_fee_bucket",
        "e.monthly_fee_bucket_2",
        "e.event_seq_2",
        "e.first_known_monthly_fee_bucket",
        "e.monthly_fee_bucket_2_percent_rank",
        "e.first_known_next_monthly_fee",
        "e.row_seq",
        "e.customer_id_sum_event_seq_2",
        "e.monthly_fee_bucket_2_rank_percent_rank",
        F.col("b.age"),
        F.col("b.email"),
        F.col("b.name"),
    )
)

totals = (
    enriched_subscriptions
    .withColumn("row_seq_tier", F.when(F.col("row_seq") >= 9, "PREMIUM").otherwise("STANDARD"))
    .cube("row_seq_tier", "customer_id")
    .agg(
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("first_known_monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("first_known_monthly_fee_bucket_quartiles"),
        F.avg("monthly_fee_bucket_2").alias("avg_monthly_fee_bucket_2"),
        F.sum("monthly_fee_bucket").alias("total_monthly_fee_bucket"),
        F.percentile_approx("first_known_next_monthly_fee", [0.25, 0.5, 0.75]).alias("first_known_next_monthly_fee_quartiles"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "row_seq_tier",
        "age_quartiles",
        "event_count",
        "first_known_monthly_fee_bucket_quartiles",
        "total_monthly_fee_bucket",
        "gid",
    )
    .orderBy(F.col("row_seq_tier").desc())
)
