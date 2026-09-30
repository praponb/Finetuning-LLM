from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id", "subscription_id")
w3 = Window.partitionBy("plan").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device", "customer_id").orderBy(F.col("subscription_id").desc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("browser", "customer_id")
w7 = Window.orderBy("seq_no", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_window = (
    df_sessions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    with_window
    .withColumn("monthly_fee_rank", F.rank().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("last_known_subscription_id", F.last("subscription_id", ignorenulls=True).over(w3))
    .withColumn("subscription_id_cume_dist", F.cume_dist().over(w4))
    .withColumn("monthly_fee_max_7d", F.max("monthly_fee").over(w5))
)

with_windows = (
    scored
    .select(
        "*",
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
        F.sum("monthly_fee_rank").over(w7).alias("running_monthly_fee_rank"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

kept_sessions = (
    kept
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

kept_sessions_2 = (
    kept_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

joined = (
    kept_sessions_2
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.segment"), F.col("b.credit_limit"))
)

result = (
    joined
    .select(
        "id",
        "started_at",
        "subscription_id",
        "monthly_fee",
        "monthly_fee_rank",
        "last_known_subscription_id",
        "subscription_id_cume_dist",
        "monthly_fee_max_7d",
        "seq_no_share",
    )
)
