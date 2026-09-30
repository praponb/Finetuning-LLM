from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("credit_limit").between(8900, 12300))
    .withColumn("country_group", F.when(F.col("country").isin(["DE", "TH"]), "Priority").otherwise("Other"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.segment",
        "s.is_active",
        F.col("c.country").alias("session_country"),
        F.col("c.device").alias("session_device"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.discount") > 0.21), "left_semi")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"))
)

result = (
    latest_match
    .select("id", "session_device", "account_type")
    .orderBy(F.col("session_device").desc())
)
