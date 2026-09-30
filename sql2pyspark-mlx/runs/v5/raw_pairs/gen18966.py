from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_status")
w_recent = Window.partitionBy("country", "city").orderBy("age", "id")
w_hist = Window.partitionBy("account_account_type")
w_grp = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_first())
w_recent_2 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") <= F.col("s.signup_date")), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.account_type").alias("account_account_type"),
        F.col("c.status").alias("account_status"),
    )
)

scored = (
    with_last
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.ntile(4).over(w_recent).alias("age_bucket"),
        F.corr("age", "credit_limit").over(w_hist).alias("corr_age_credit_limit"),
        F.percent_rank().over(w_grp).alias("age_percent_rank"),
        F.ntile(4).over(w_recent_2).alias("age_bucket_2"),
    )
)

kept_customers = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") != "PAUSED"), "left_anti")
)

result = (
    kept_customers
    .groupBy(F.date_trunc("week", "signup_date").alias("week_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("age_bucket_2"), 2).alias("avg_age_bucket_2"),
        F.count(F.when(F.col("account_status") == "FROZEN", 1)).alias("frozen_count"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
    )
    .filter(F.col("uk_rows") > 1)
)
