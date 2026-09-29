from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("country_length").desc(), "id")
w_running = Window.partitionBy("segment")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-17").cast("date")) & F.col("email").like("%@outlook.com"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat_ws(",", "email", "country").alias("email_country_key"),
        F.length("country").alias("country_length"),
    )
)

valid = (
    filtered
    .filter((F.col("signup_date") > F.lit("2023-03-10").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "email_country_key",
        "country_length",
        F.concat("city", F.lit("/"), "name").alias("city_name"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    deduped
    .select("id", "country")
    .subtract(
        df_customers
        .filter(~F.col("city").isin(["Bangkok", "Tokyo", "Berlin"]))
        .select("id", "country")
    )
)

verified = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") != "CLOSED"), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") >= F.col("s.signup_date")), "left_semi")
)

scored = (
    kept
    .filter(F.col("credit_limit") <= 7300)
    .withColumn("segment_max_credit_limit", F.max("credit_limit").over(w_running))
)

summary = (
    scored
    .groupBy("country", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.percentile_approx("segment_max_credit_limit", [0.25, 0.5, 0.75]).alias("segment_max_credit_limit_quartiles"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .filter(F.col("num_segments") >= 1)
)

result = (
    summary
    .select(
        "country",
        "year_start",
        "num_segments",
        F.when(F.col("country").rlike("^(JP|UK)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("country_zone"),
    )
)
