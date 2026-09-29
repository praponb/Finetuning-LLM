from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("subscription_start_date", "id")
w3 = Window.partitionBy("segment").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit_avg_6").desc_nulls_first())
w6 = Window.partitionBy("city").orderBy(F.col("greatest_credit_limit_age").asc_nulls_first())

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-04-15").cast("date")) & F.col("city").isNotNull())
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
        F.greatest("credit_limit", "age").alias("greatest_credit_limit_age"),
        F.trim("country").alias("clean_country"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.end_date"),
        F.col("c.start_date").alias("subscription_start_date"),
    )
)

with_windows = (
    latest_match
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("credit_limit_avg_6", F.avg("credit_limit").over(w3))
)

matched_keys = (
    with_windows
    .select("country")
    .intersect(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("country")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    not_flagged
    .withColumn("credit_limit_avg_6_share", F.round((F.col("credit_limit_avg_6") / F.sum("credit_limit_avg_6").over(w4)), 4))
    .withColumn("credit_limit_avg_6_cume_dist", F.cume_dist().over(w5))
    .withColumn("greatest_credit_limit_age_rank", F.rank().over(w6))
)

result = (
    scored
    .groupBy(F.date_trunc("WEEK", "signup_date").alias("week_start"))
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.count("*").alias("row_count"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.round(F.sum("credit_limit_avg_6_share") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("credit_limit_avg_6_share_per_th"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
    )
    .orderBy(F.col("credit_limit_avg_6_share_per_th").desc_nulls_first())
)
