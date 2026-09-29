from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_first())
w2 = Window.partitionBy("country")
w3 = Window.orderBy(F.col("corr_credit_limit_age").desc(), "id")
w4 = Window.partitionBy("country").orderBy(F.col("corr_age_credit_limit").desc())

metrics = (
    df_customers
    .select(
        "*",
        F.cume_dist().over(w).alias("credit_limit_cume_dist"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
        F.corr("age", "credit_limit").over(w2).alias("corr_age_credit_limit"),
    )
)

filtered = (
    metrics
    .filter(F.col("signup_date") >= F.lit("2023-05-18").cast("date"))
    .select(
        "*",
        (F.col("corr_age_credit_limit") - F.col("corr_credit_limit_age")).alias("corr_age_credit_limit_minus_corr_credit_limit_age"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

cleaned = (
    filtered
    .filter(F.col("segment").isNotNull() & F.col("credit_limit_cume_dist").between(0.3, 0.74))
    .withColumn("credit_limit_minus_age", (F.col("credit_limit") - F.col("age")))
    .withColumn("segment_upper", F.upper("segment"))
)

enriched = (
    cleaned
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "corr_credit_limit_age",
        "corr_age_credit_limit",
        "resolved_city",
        "credit_limit_minus_age",
        F.ntile(10).over(w3).alias("corr_credit_limit_age_bucket"),
        F.rank().over(w4).alias("corr_age_credit_limit_rank"),
    )
)

summary = (
    enriched
    .cube(
        F.date_trunc("YEAR", "signup_date").alias("year_start"),
        F.when(F.col("corr_age_credit_limit_rank") >= 7, "LARGE").otherwise("SMALL").alias("corr_age_credit_limit_rank_tier"),
        F.when(F.col("credit_limit_minus_age") >= 88, "LARGE").otherwise("SMALL").alias("credit_limit_minus_age_tier"),
    )
    .agg(
        F.avg(F.when(F.col("segment") == "SMB", F.col("corr_credit_limit_age"))).alias("smb_avg_corr_credit_limit_age"),
        F.percentile_approx("corr_credit_limit_age", 0.95).alias("corr_credit_limit_age_p95"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
)

result = (
    summary
    .select(
        "year_start",
        "credit_limit_minus_age_tier",
        "smb_avg_corr_credit_limit_age",
        "corr_credit_limit_age_p95",
    )
    .orderBy("smb_avg_corr_credit_limit_age", "corr_credit_limit_age_p95")
)
