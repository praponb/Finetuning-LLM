from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment", "country")
w_recent = Window.partitionBy("country")
w_hist = Window.orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country") == "US"), "left_anti")
)

metrics = (
    eligible
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w))
    .withColumn("corr_credit_limit_age_2", F.corr("credit_limit", "age").over(w2))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4))
)

scored = (
    metrics
    .filter(F.col("corr_credit_limit_age_2") >= 0.1)
    .select(
        "id",
        "segment",
        "signup_date",
        "corr_credit_limit_age_2",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.corr("credit_limit", "age").over(w_recent).alias("corr_credit_limit_age_3"),
        F.sum("credit_limit_share").over(w_hist).alias("cumulative_credit_limit_share"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.signup_date",
        "s.corr_age_credit_limit",
        "s.corr_credit_limit_age_3",
        F.col("c.balance"),
    )
)

result = (
    with_last
    .select("id", "corr_age_credit_limit", "corr_credit_limit_age_3", "balance")
    .orderBy(F.col("corr_age_credit_limit").desc_nulls_first(), "id")
)
