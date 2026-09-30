from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("status_length").asc_nulls_last())
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w_grp = Window.orderBy(F.col("status_length").desc())
w_part = Window.orderBy(F.col("status_length").desc(), "id")
w5 = Window.partitionBy("customer_id").orderBy("status_length_rank", "id")
w6 = Window.partitionBy("status", "customer_id").orderBy("status_length", "id")
w7 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("plan", "customer_id").orderBy("status_length_rank", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("status").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("plan").orderBy(F.col("next_status_length").desc_nulls_first(), "id")
w11 = Window.partitionBy("customer_id").orderBy(F.col("cumulative_status_length_rank").desc(), "id")
w12 = Window.partitionBy("status", "customer_id").orderBy(F.col("next_status_length").desc_nulls_first(), "id")

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-11-28").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.length("status").alias("status_length"),
    )
)

with_windows = (
    base
    .withColumn("status_length_rank", F.rank().over(w))
    .withColumn("next_status_length", F.lead("status_length", 2).over(w2))
    .withColumn("status_length_rank_2", F.rank().over(w_grp))
    .withColumn("status_length_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("status_length_bucket", F.ntile(5).over(w_part))
)

enriched = (
    with_windows
    .withColumn("event_seq", F.row_number().over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("first_known_status_length_rank", F.first("status_length_rank", ignorenulls=True).over(w7))
    .withColumn("status_length_avg_3", F.avg("status_length").over(w8))
    .withColumn("cumulative_status_length_rank", F.sum("status_length_rank").over(w9))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    ranked_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.name"), F.col("b.signup_date"))
)

ranked_subscriptions_2 = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w12))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_subscriptions_2
    .cube(
        F.when(F.col("status_length_bucket") >= 9, "LARGE").otherwise("SMALL").alias("status_length_bucket_tier"),
        F.when(F.col("status_length_percent_rank") >= 0.7, "PREMIUM").otherwise("STANDARD").alias("status_length_percent_rank_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("status_length_bucket"))).alias("expired_total_status_length_bucket"),
        F.percentile_approx("first_known_status_length_rank", 0.9).alias("first_known_status_length_rank_p90"),
        F.count(F.when(F.col("status") == "PAUSED", 1)).alias("paused_count"),
        F.min("status_length_percent_rank").alias("min_status_length_percent_rank"),
        F.count_if(F.col("status") == "CANCELLED").alias("cancelled_events"),
    )
)
