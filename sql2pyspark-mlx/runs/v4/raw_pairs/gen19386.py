from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_sessions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    matched
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at")), "left_anti")
)

metrics = (
    kept
    .withColumn("subscription_id_avg_2", F.avg("subscription_id").over(w))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w2))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "subscription_id",
        "monthly_fee",
        "subscription_id_avg_2",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "CORE").otherwise("GROUP_B").alias("device_zone"),
    )
)
