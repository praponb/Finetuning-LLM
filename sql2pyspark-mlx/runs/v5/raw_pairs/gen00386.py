from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("credit_limit", "id")
w3 = Window.partitionBy("plan").orderBy("credit_limit", "id")
w4 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("country").orderBy(F.col("credit_limit_bucket").desc_nulls_first())
w_grp = Window.orderBy(F.col("seq_no").asc_nulls_last())
w_hist = Window.partitionBy("country")

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-09-04").cast("date"))
    .withColumn("segment_title", F.initcap(F.trim("segment")))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("N/A")))
)

latest_match = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.plan"), F.col("c.end_date"))
)

scored = (
    latest_match
    .select(
        "id",
        "name",
        "email",
        "country",
        "age",
        "segment_title",
        "segment_filled",
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w3).alias("credit_limit_bucket"),
        F.avg("age").over(w4).alias("age_avg_6_prior"),
    )
)

latest = (
    scored
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept_keys = (
    latest
    .select("country", "id")
    .exceptAll(
        df_customers
        .filter(F.length("name") >= 8)
        .select("country", "id")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "name",
        "email",
        "country",
        "segment_title",
        "seq_no",
        "credit_limit_bucket",
        "age_avg_6_prior",
        F.percent_rank().over(w_grp).alias("seq_no_percent_rank"),
        F.min("seq_no").over(w_hist).alias("country_min_seq_no"),
        F.avg("age").over(w_hist).alias("country_avg_age"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("seq_no_percent_rank") > 0.2)
    .select(
        "name",
        "email",
        "country",
        "segment_title",
        "age_avg_6_prior",
        "seq_no_percent_rank",
        "country_min_seq_no",
        F.least("country_avg_age", "age_avg_6_prior").alias("least_country_avg_age_age_avg_6_prior"),
        (F.col("country_avg_age") / F.nullif("seq_no_percent_rank", F.lit(0))).alias("country_avg_age_per_seq_no_percent_rank"),
    )
)

result = (
    cleaned
    .withColumn("age_avg_6_prior_tier", F.when(F.col("age_avg_6_prior") >= 61, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["age_avg_6_prior_tier", "country_min_seq_no"], ["age_avg_6_prior_tier"]],
        "age_avg_6_prior_tier",
        "country_min_seq_no",
    )
    .agg(
        F.percentile_approx("least_country_avg_age_age_avg_6_prior", 0.5).alias("least_country_avg_age_age_avg_6_prior_p50"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
