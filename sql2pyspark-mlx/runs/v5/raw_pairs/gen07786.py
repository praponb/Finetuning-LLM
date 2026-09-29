from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("device", "segment")
w_recent = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("device", "account_type")
w6 = Window.partitionBy("city").orderBy(F.col("corr_age_credit_limit").desc_nulls_last(), "id")

eligible = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device") != "desktop"), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"))
)

with_latest_customers = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"), F.col("c.started_at").alias("session_started_at"))
)

enriched = (
    with_latest_customers
    .filter(F.col("device") == "mobile")
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "device",
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
        F.avg("credit_limit").over(w_recent).alias("credit_limit_avg_2"),
        F.corr("age", "credit_limit").over(w5).alias("corr_age_credit_limit_2"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "is_active",
        "credit_limit",
        "age",
        "credit_limit_avg_2",
        "corr_age_credit_limit_2",
    )
)

result = (
    deduped
    .select("id", "credit_limit", "corr_age_credit_limit_2")
    .unpivot(
        ids=["id"],
        values=["credit_limit", "corr_age_credit_limit_2"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
