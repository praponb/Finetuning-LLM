from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country")

cleaned = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "*",
        F.when(F.col("city") == "Bangkok", 1).otherwise(0).alias("is_bangkok"),
        (F.col("age") - F.col("credit_limit")).alias("age_minus_credit_limit"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

valid = (
    cleaned
    .filter(F.col("is_active"))
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "is_bangkok",
        "age_minus_credit_limit",
        "segment_clean",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

remaining_keys = (
    valid
    .select("country", "id")
    .subtract(
        df_customers
        .filter(F.col("country") != "US")
        .select("country", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.signup_date")), "left_anti")
)

metrics = not_flagged.withColumn("country_sum_age", F.sum("age").over(w_part))

kept = (
    metrics
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status").isNotNull()), "left_anti")
)

base = (
    kept
    .filter(F.col("signup_date") > F.lit("2023-08-13").cast("date"))
    .withColumn("email_prefix", F.substring("email", 1, 3))
)

result = (
    base
    .groupBy(
        F.when(F.col("country_sum_age") >= 273, "LARGE").otherwise("SMALL").alias("country_sum_age_tier"),
        "country",
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("is_singapore", 0.95).alias("is_singapore_p95"),
        F.bit_or("age").alias("age_bit_or"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.round(F.sum("is_singapore") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("is_singapore_per_jp"),
    )
    .filter(F.col("is_singapore_p95") >= 0)
)
