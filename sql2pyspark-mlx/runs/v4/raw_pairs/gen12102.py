from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id", "device").orderBy("signup_date", "id")
w3 = Window.partitionBy("city").orderBy("started_at", "id")
w4 = Window.partitionBy("city").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2024-04-13").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("country").alias("clean_country"),
        F.substring("device", 1, 2).alias("device_prefix"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country").alias("customer_country"), F.col("c.signup_date"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

joined = (
    with_last
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"))
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

gaps = (
    deduped
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w4))

result = (
    islands
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "device_prefix",
        "customer_country",
        "web_event_page",
        "web_event_duration_sec",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy("customer_id")
)
