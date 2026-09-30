from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w_ordered = Window.partitionBy("city").orderBy("credit_limit_rounded", "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-05-21").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
    )
)

cleaned = (
    filtered
    .filter(F.col("city").isNotNull() & F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        (F.col("credit_limit_rounded") - F.col("credit_limit")).alias("credit_limit_rounded_minus_credit_limit"),
    )
)

metrics = (
    cleaned
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.ntile(10).over(w2).alias("credit_limit_bucket"),
        F.lag("credit_limit", 2, 0).over(w_ordered).alias("prev_credit_limit"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_semi")
)

scored = (
    eligible
    .select(
        "id",
        "country",
        "is_active",
        "credit_limit_bucket",
        "prev_credit_limit",
        F.round((F.col("credit_limit_bucket") / F.sum("credit_limit_bucket").over(w4)), 4).alias("credit_limit_bucket_share"),
        F.sum("credit_limit_cume_dist").over(w5).alias("cumulative_credit_limit_cume_dist"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.start_date").alias("subscription_start_date"))
)

grouped = (
    with_last
    .groupBy("country")
    .agg(
        F.percentile_approx("credit_limit_bucket", 0.95).alias("credit_limit_bucket_p95"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
    )
)

result = (
    grouped
    .select(
        "country",
        "credit_limit_bucket_p95",
        "uk_count",
        F.when(F.col("country").rlike("^(JP|DE)$"), "GROUP_A").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
    )
)
