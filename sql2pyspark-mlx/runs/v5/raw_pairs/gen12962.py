from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_status")
w3 = Window.partitionBy("country", "segment")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date"), F.col("c.status").alias("account_status"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.opened_date")), "left_anti")
)

scored = (
    kept
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w2)), 4))
    .withColumn("country_avg_credit_limit", F.avg("credit_limit").over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.opened_date")), "left_anti")
)

with_latest_customers = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.age_share",
        "s.country_avg_credit_limit",
        F.col("c.started_at"),
    )
)

result = (
    with_latest_customers
    .select("id", "email", "age", "age_share", "country_avg_credit_limit", "started_at")
    .orderBy("country_avg_credit_limit")
)
