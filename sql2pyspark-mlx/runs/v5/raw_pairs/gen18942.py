from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id")
w2 = Window.partitionBy("country", "city").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("segment")

valid = (
    df_customers
    .filter(F.col("city").isNotNull())
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
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("NONE")).alias("city_filled"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "name",
        "city",
        "segment",
        "city_lower",
        F.row_number().over(w).alias("row_seq"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_3_prior"),
        F.avg("credit_limit").over(w3).alias("segment_avg_credit_limit"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("credit_limit_avg_3_prior").isNotNull())
    .select(
        "id",
        "name",
        "row_seq",
        "credit_limit_avg_3_prior",
        "segment_avg_credit_limit",
        F.col("segment_avg_credit_limit").cast("string").alias("segment_avg_credit_limit_num"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([10, 14, 15, 32])), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("credit_limit_avg_3_prior") >= 15961, "HIGH").otherwise("LOW").alias("credit_limit_avg_3_prior_tier"),
    )
    .agg(
        F.percentile_approx("credit_limit_avg_3_prior", 0.9).alias("credit_limit_avg_3_prior_p90"),
        F.max("segment_avg_credit_limit").alias("max_segment_avg_credit_limit"),
        F.round(F.avg("credit_limit_avg_3_prior"), 2).alias("avg_credit_limit_avg_3_prior"),
        F.percentile_approx("credit_limit_avg_3_prior", 0.9).alias("credit_limit_avg_3_prior_p90_2"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("max_segment_avg_credit_limit").desc_nulls_first())
)
