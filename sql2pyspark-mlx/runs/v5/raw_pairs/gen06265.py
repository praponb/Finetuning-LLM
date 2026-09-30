from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("subscription_status"))
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.browser").alias("session_browser"),
        F.col("c.device").alias("session_device"),
    )
)

with_latest = (
    with_last_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.segment",
        "s.is_active",
        "s.session_browser",
        "s.session_device",
        F.col("c.browser"),
    )
)

with_last_customers_2 = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.is_active",
        "s.browser",
        F.col("c.browser").alias("session_browser_2"),
    )
)

result = (
    with_last_customers_2
    .select(
        "id",
        "browser",
        "session_browser_2",
        F.when(F.col("session_browser_2").rlike("^(Safari|Firefox)$"), "GROUP_A").otherwise("EXTENDED").alias("session_browser_2_zone"),
    )
    .orderBy(F.col("session_browser_2_zone").asc_nulls_first(), F.col("browser").desc())
)
