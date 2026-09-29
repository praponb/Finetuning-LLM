from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city", "segment").orderBy("credit_limit_per_age", "id")
w4 = Window.partitionBy("country").orderBy(F.col("least_credit_limit_per_age_row_seq").desc_nulls_last())
w5 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())

valid = (
    df_customers
    .filter(~F.col("segment").eqNullSafe(F.lit("SMB")))
    .select(
        "*",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("city", F.lit("OTHER")).alias("resolved_city"),
    )
)

with_windows = (
    valid
    .filter(F.col("age").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        "credit_limit_per_age",
        "days_since_signup_date",
        F.row_number().over(w).alias("row_seq"),
        F.min("days_since_signup_date").over(w2).alias("country_min_days_since_signup_date"),
        F.ntile(3).over(w3).alias("credit_limit_per_age_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.col("country") == "JP")
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isNull()), "left_semi")
)

valid_customers = (
    kept
    .filter((F.col("signup_date") > F.lit("2023-09-05").cast("date")) & F.col("country").isin(["TH", "US", "JP"]))
    .withColumn("credit_limit_per_age_bucket_per_days_since_signup_date", (F.col("credit_limit_per_age_bucket") / F.nullif("days_since_signup_date", F.lit(0))))
    .withColumn("least_credit_limit_per_age_row_seq", F.least("credit_limit_per_age", "row_seq"))
)

metrics = (
    valid_customers
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "credit_limit",
        "credit_limit_per_age",
        "row_seq",
        "country_min_days_since_signup_date",
        "credit_limit_per_age_bucket_per_days_since_signup_date",
        F.sum("days_since_signup_date").over(w2).alias("country_sum_days_since_signup_date"),
        F.cume_dist().over(w4).alias("least_credit_limit_per_age_row_seq_cume_dist"),
        F.cume_dist().over(w5).alias("credit_limit_cume_dist"),
        F.count("credit_limit_per_age_bucket_per_days_since_signup_date").over(w2).alias("country_count_credit_limit_per_age_bucket_per_days_since_signup_date"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_semi")
)

daily = (
    not_flagged
    .groupBy("country")
    .agg(
        F.percentile_approx("credit_limit_cume_dist", 0.95).alias("credit_limit_cume_dist_p95"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.percentile_approx("credit_limit_per_age_bucket_per_days_since_signup_date", [0.25, 0.5, 0.75]).alias("credit_limit_per_age_bucket_per_days_since_signup_date_quartiles"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.percentile_approx("country_count_credit_limit_per_age_bucket_per_days_since_signup_date", [0.25, 0.5, 0.75]).alias("country_count_credit_limit_per_age_bucket_per_days_since_signup_date_quartiles"),
    )
)

result = (
    daily
    .select(
        "country",
        "sg_events",
        "credit_limit_per_age_bucket_per_days_since_signup_date_quartiles",
        "credit_limit_stddev",
        "country_count_credit_limit_per_age_bucket_per_days_since_signup_date_quartiles",
    )
    .orderBy("country")
)
