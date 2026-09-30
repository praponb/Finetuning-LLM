from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit_whole", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("country", "segment").orderBy(F.col("credit_limit_whole").desc())
w3 = Window.partitionBy("city", "country")
w4 = Window.orderBy(F.col("credit_limit_whole_rank").desc(), "id")
w5 = Window.partitionBy("segment")
w6 = Window.partitionBy("country").orderBy(F.col("credit_limit_whole").desc(), "id")
w7 = Window.partitionBy("country", "city")
w_recent = Window.partitionBy("country").orderBy("credit_limit_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("country")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w11 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w12 = Window.partitionBy("session_device").orderBy("credit_limit_whole_rank_bucket", "id")

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("*", F.ceil("credit_limit").alias("credit_limit_whole"))
)

with_windows = (
    cleaned
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "credit_limit_whole",
        F.avg("age").over(w).alias("age_avg_3_prior"),
        F.rank().over(w2).alias("credit_limit_whole_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "credit_limit_whole",
        "credit_limit_whole_rank",
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age"),
        F.ntile(4).over(w4).alias("credit_limit_whole_rank_bucket"),
        F.stddev_samp("age_avg_3_prior").over(w5).alias("segment_stddev_samp_age_avg_3_prior"),
        F.row_number().over(w6).alias("event_seq"),
        F.corr("age", "credit_limit").over(w7).alias("corr_age_credit_limit"),
    )
)

metrics_customers = (
    metrics
    .withColumn("running_max_credit_limit_whole_rank", F.max("credit_limit_whole_rank").over(w_recent))
    .withColumn("running_sum_credit_limit_whole_rank", F.sum("credit_limit_whole_rank").over(w_recent))
    .withColumn("running_avg_credit_limit_whole_rank", F.avg("credit_limit_whole_rank").over(w_recent))
    .withColumn("credit_limit_whole_rank_share", F.round((F.col("credit_limit_whole_rank") / F.sum("credit_limit_whole_rank").over(w9)), 4))
)

with_latest = (
    metrics_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit_whole_rank",
        "s.credit_limit_whole_rank_bucket",
        "s.segment_stddev_samp_age_avg_3_prior",
        "s.event_seq",
        "s.corr_age_credit_limit",
        "s.running_sum_credit_limit_whole_rank",
        "s.running_avg_credit_limit_whole_rank",
        "s.credit_limit_whole_rank_share",
        F.col("c.status").alias("account_status"),
    )
)

with_latest_customers = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.credit_limit_whole_rank_bucket",
        "s.segment_stddev_samp_age_avg_3_prior",
        "s.event_seq",
        "s.credit_limit_whole_rank_share",
        "s.account_status",
        F.col("c.device").alias("session_device"),
        F.col("c.country").alias("session_country"),
    )
)

latest = (
    with_latest_customers
    .withColumn("row_num", F.row_number().over(w12))
    .filter(F.col("row_num") == 1)
    .select("id", "credit_limit_whole_rank_share", "session_country")
)

result = (
    latest
    .groupBy(
        F.when(F.col("credit_limit_whole_rank_share") >= 0.88, "LARGE").otherwise("SMALL").alias("credit_limit_whole_rank_share_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit_whole_rank_share", 0.5).alias("credit_limit_whole_rank_share_p50"),
        F.count(F.when(F.col("session_country") == "DE", 1)).alias("de_rows"),
        F.sum(F.when(F.col("session_country") == "TH", F.col("credit_limit_whole_rank_share")).otherwise(0)).alias("th_credit_limit_whole_rank_share"),
        F.percentile_approx("credit_limit_whole_rank_share", [0.25, 0.5, 0.75]).alias("credit_limit_whole_rank_share_quartiles"),
        F.count(F.when(F.col("session_country") == "US", 1)).alias("us_count"),
    )
)
