from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date_month_end").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("signup_date_month_end").desc_nulls_last())
w3 = Window.partitionBy("city", "segment").orderBy("signup_date_month_end", "id")
w4 = Window.partitionBy("city", "segment").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2024-01-08").cast("date"))
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.last_day("signup_date").alias("signup_date_month_end"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

valid_customers = (
    ranked
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "city",
        "segment",
        "credit_limit",
        "age",
        "signup_date_month_end",
        "dedup_rank",
        F.concat_ws("/", "name", "city").alias("name_city_key"),
    )
)

kept = (
    valid_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.signup_date_month_end")), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.lead("age", 1).over(w3).alias("next_age"),
        F.ntile(4).over(w4).alias("credit_limit_bucket"),
        F.rank().over(w5).alias("credit_limit_rank"),
    )
)

stats = (
    scored
    .groupBy(
        F.date_trunc("QUARTER", "signup_date_month_end").alias("quarter_start"),
        F.when(F.col("credit_limit") >= 15000, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("next_age"))).alias("london_total_next_age"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_count"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.percentile_approx("next_age", 0.5).alias("next_age_p50"),
        F.max(F.when(F.col("segment") == "Consumer", F.col("credit_limit_rank"))).alias("consumer_max_credit_limit_rank"),
    )
    .filter(F.col("consumer_rows") > 1)
)

result = (
    stats
    .select(
        "quarter_start",
        "credit_limit_tier",
        "corr_credit_limit_age",
        "next_age_p50",
    )
    .orderBy(F.col("credit_limit_tier").desc())
)
