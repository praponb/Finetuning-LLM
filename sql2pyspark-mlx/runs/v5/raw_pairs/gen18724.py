from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-6, -1)
w_ordered = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w4 = Window.partitionBy("status", "city").orderBy("signup_date", "id")

latest_match = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.segment",
        "s.signup_date",
        "s.credit_limit",
        "s.age",
        F.col("c.monthly_fee"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isNotNull()), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.signup_date",
        "s.credit_limit",
        "s.age",
        "s.monthly_fee",
        F.col("c.status"),
    )
)

with_windows = (
    with_last
    .select(
        "id",
        "city",
        "credit_limit",
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_6_prior"),
        F.cume_dist().over(w_ordered).alias("monthly_fee_cume_dist"),
        F.lag("credit_limit", 2).over(w4).alias("prev_credit_limit"),
    )
)

not_flagged_customers = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNull()), "left_anti")
)

result = (
    not_flagged_customers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows_2"),
        F.percentile_approx("credit_limit_avg_6_prior", 0.9).alias("credit_limit_avg_6_prior_p90"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("prev_credit_limit"), 2).alias("avg_prev_credit_limit"),
    )
)
