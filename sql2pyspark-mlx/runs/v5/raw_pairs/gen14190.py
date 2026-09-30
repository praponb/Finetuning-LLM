from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(~F.col("segment").eqNullSafe(F.lit("Consumer")))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

eligible = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.signup_date")), "left_semi")
)

deduped = (
    eligible
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "email_user",
        "resolved_city",
    )
)

with_latest = (
    deduped
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device").alias("session_device"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.segment",
        "s.email_user",
        "s.resolved_city",
        F.col("c.account_type"),
        F.col("c.status").alias("account_status"),
    )
)

result = (
    latest_match
    .select("id", "name", "segment", "resolved_city", "account_type", "account_status")
    .orderBy(F.col("id").desc_nulls_first())
    .limit(10)
)
