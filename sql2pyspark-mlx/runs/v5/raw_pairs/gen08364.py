from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country").orderBy("subscription_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("running_count_credit_limit").desc_nulls_first())
w4 = Window.orderBy(F.col("running_avg_credit_limit").desc_nulls_first())

latest_match = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        F.col("c.status").alias("subscription_status"),
        F.col("c.end_date").alias("subscription_end_date"),
    )
)

with_windows = (
    latest_match
    .withColumn("running_sum_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("running_count_credit_limit", F.count("credit_limit").over(w2))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w2))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w2))
)

kept_keys = (
    with_windows
    .select("country")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("country")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

with_windows_customers = (
    kept_rows
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "signup_date",
        "is_active",
        "subscription_status",
        "running_sum_credit_limit",
        "running_count_credit_limit",
        F.percent_rank().over(w3).alias("running_count_credit_limit_percent_rank"),
        F.percent_rank().over(w4).alias("running_avg_credit_limit_percent_rank"),
    )
)

result = (
    with_windows_customers
    .select(
        "subscription_status",
        "running_avg_credit_limit_percent_rank",
        F.col("running_count_credit_limit_percent_rank").alias("RUNNING_COUNT_CREDIT_LIMIT_PERCENT_RANK_VAL"),
        F.col("running_sum_credit_limit").alias("RUNNING_SUM_CREDIT_LIMIT"),
    )
    .unpivot(
        ids=["subscription_status"],
        values=["running_avg_credit_limit_percent_rank", "RUNNING_COUNT_CREDIT_LIMIT_PERCENT_RANK_VAL", "RUNNING_SUM_CREDIT_LIMIT"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
