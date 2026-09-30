from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("signup_date")
w2 = Window.partitionBy("city")
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country")
w_recent = Window.partitionBy("city").orderBy(F.col("age").desc())
w6 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.signup_date")), "left_anti")
)

latest = (
    kept
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    latest
    .select(
        "*",
        F.corr("age", "credit_limit").over(w2).alias("corr_age_credit_limit"),
        F.sum("credit_limit").over(w3).alias("cumulative_credit_limit"),
        F.stddev_samp("age").over(w4).alias("country_stddev_samp_age"),
        F.percent_rank().over(w_recent).alias("age_percent_rank"),
        F.ntile(10).over(w6).alias("age_bucket"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") <= F.col("s.signup_date")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "email",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "corr_age_credit_limit",
        "age_percent_rank",
        "age_bucket",
    )
    .orderBy(F.col("age_percent_rank").desc_nulls_first())
)
