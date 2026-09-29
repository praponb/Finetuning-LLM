from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("country").orderBy(F.col("credit_limit_plus_age").desc(), "id")
w3 = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("credit_limit_plus_age", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("credit_limit_plus_age").desc(), "id")

cleaned = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-02-16")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        F.initcap(F.trim("country")).alias("country_title"),
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.start_date").alias("subscription_start_date"),
        F.col("c.plan"),
    )
)

metrics = (
    latest_match
    .filter(F.col("email").like("%@company.co"))
    .select(
        "*",
        F.ntile(10).over(w_running).alias("credit_limit_plus_age_bucket"),
        F.last("plan", ignorenulls=True).over(w3).alias("last_known_plan"),
        F.lag("credit_limit_plus_age", 1).over(w4).alias("prev_credit_limit_plus_age"),
    )
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "credit_limit_plus_age",
        "credit_limit_plus_age_bucket",
        "last_known_plan",
        "prev_credit_limit_plus_age",
    )
)

result = (
    latest
    .groupBy("country", "segment")
    .agg(
        F.percentile_approx("prev_credit_limit_plus_age", 0.9).alias("prev_credit_limit_plus_age_p90"),
        F.percentile_approx("credit_limit_plus_age_bucket", 0.5).alias("credit_limit_plus_age_bucket_p50"),
        F.count("*").alias("row_count"),
        F.percentile_approx("credit_limit_plus_age_bucket", 0.9).alias("credit_limit_plus_age_bucket_p90"),
    )
    .orderBy(F.col("row_count").asc_nulls_last())
)
