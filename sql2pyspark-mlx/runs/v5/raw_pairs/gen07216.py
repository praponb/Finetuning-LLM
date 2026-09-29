from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device", "customer_id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .withColumn("device_lower", F.lower("device"))
)

enriched = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.is_active"))
    .select("a.*", F.col("p.email"), F.col("p.city"), F.col("p.credit_limit"))
)

enriched_sessions = (
    enriched
    .withColumn("running_count_credit_limit", F.count("credit_limit").over(w))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w))
    .withColumn("running_max_credit_limit", F.max("credit_limit").over(w))
    .withColumn("running_avg_credit_limit", F.avg("credit_limit").over(w))
    .withColumn("device_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w2))
)

latest_match = (
    enriched_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

latest_match_sessions = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

kept = (
    latest_match_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    kept
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "email",
        "credit_limit",
        "running_min_credit_limit",
        "running_max_credit_limit",
        F.col("max_subscription_monthly_fee"),
    )
)
