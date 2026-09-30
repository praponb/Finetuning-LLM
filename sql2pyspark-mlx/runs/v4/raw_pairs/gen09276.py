from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("running_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("segment").orderBy("age_avg_2_prior")
w5 = Window.partitionBy("city").orderBy("age_avg_2_prior_percent_rank", "id")
w6 = Window.partitionBy("country", "city").orderBy(F.col("age_avg_2_prior").desc(), "id")
w7 = Window.partitionBy("country")
w8 = Window.orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("country", "segment").orderBy(F.col("age_avg_2_prior_percent_rank").desc(), "id")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

with_windows = (
    df_customers
    .withColumn("running_age", F.sum("age").over(w))
    .withColumn("age_avg_2_prior", F.avg("age").over(w2))
)

metrics = (
    with_windows
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
        F.percent_rank().over(w_part).alias("age_avg_2_prior_percent_rank"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lead("age_avg_2_prior", 1).over(w5).alias("next_age_avg_2_prior"),
        F.ntile(5).over(w6).alias("age_avg_2_prior_bucket"),
        F.stddev_samp("credit_limit").over(w7).alias("country_stddev_samp_credit_limit"),
        F.avg("age_avg_2_prior_percent_rank").over(w8).alias("age_avg_2_prior_percent_rank_avg_5"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

filtered = (
    enriched
    .filter(F.col("signup_date") > F.lit("2023-11-30").cast("date"))
    .withColumn("city_title", F.initcap(F.trim("city")))
)

cleaned = (
    filtered
    .filter(F.col("next_age_avg_2_prior") == 36)
    .withColumn("country_stddev_samp_credit_limit_plus_age", (F.col("country_stddev_samp_credit_limit") + F.col("age")))
    .withColumn("age_avg_2_prior_percent_rank_avg_5_filled", F.coalesce("age_avg_2_prior_percent_rank_avg_5", F.lit(0)))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"), F.col("c.status").alias("account_status"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

result = (
    kept
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "age_avg_2_prior",
    )
    .orderBy(F.col("segment").desc(), F.col("credit_limit").desc())
)
