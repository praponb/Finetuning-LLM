from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("days_since_signup_date", "id")
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("country").orderBy("next_days_since_signup_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("city", "segment")
w6 = Window.partitionBy("country").orderBy("days_since_signup_date", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("status", "channel").orderBy("age", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-02-08").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

with_windows = (
    base
    .withColumn("next_days_since_signup_date", F.lead("days_since_signup_date").over(w))
    .withColumn("prev_credit_limit", F.lag("credit_limit").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("age_avg_3_prior", F.avg("age").over(w2))
)

metrics = (
    with_windows
    .withColumn("next_days_since_signup_date_avg_2_prior", F.avg("next_days_since_signup_date").over(w3))
    .withColumn("age_avg_3_prior_avg_5", F.avg("age_avg_3_prior").over(w4))
    .withColumn("city_stddev_samp_age_avg_3_prior", F.stddev_samp("age_avg_3_prior").over(w5))
    .withColumn("next_days_since_signup_date_avg_5", F.avg("next_days_since_signup_date").over(w6))
    .withColumn("cumulative_next_days_since_signup_date", F.sum("next_days_since_signup_date").over(w7))
)

with_last = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.days_since_signup_date",
        "s.next_days_since_signup_date",
        "s.prev_credit_limit",
        "s.next_days_since_signup_date_avg_2_prior",
        "s.age_avg_3_prior_avg_5",
        "s.cumulative_next_days_since_signup_date",
        F.col("c.channel"),
        F.col("c.status"),
    )
)

metrics_customers = (
    with_last
    .filter(F.col("next_days_since_signup_date") != 67)
    .withColumn("last4_sum_cumulative_next_days_since_signup_date", F.sum("cumulative_next_days_since_signup_date").over(w9))
    .withColumn("last4_avg_cumulative_next_days_since_signup_date", F.avg("cumulative_next_days_since_signup_date").over(w9))
    .withColumn("last4_min_cumulative_next_days_since_signup_date", F.min("cumulative_next_days_since_signup_date").over(w9))
    .withColumn("last4_count_cumulative_next_days_since_signup_date", F.count("cumulative_next_days_since_signup_date").over(w9))
)

result = (
    metrics_customers
    .select(
        "id",
        "country",
        "is_active",
        "credit_limit",
        "age",
        "days_since_signup_date",
        "next_days_since_signup_date",
        "prev_credit_limit",
        "next_days_since_signup_date_avg_2_prior",
        F.when(F.col("country").rlike("^(SG|JP)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc())
)
