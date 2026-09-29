from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)

attributed = (
    metrics
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

not_flagged = (
    attributed
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page") == "/home"), "left_anti")
)

enriched = (
    not_flagged
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.signup_date"), F.col("d.credit_limit"))
)

result = (
    enriched
    .groupBy("country", F.date_trunc("month", "signup_date").alias("activity_month"))
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id"),
        F.count("*").alias("event_count"),
        F.round(F.sum("monthly_fee") / F.nullif(F.count(F.when(F.col("device") == "mobile", 1)), F.lit(0)), 2).alias("monthly_fee_per_mobile"),
    )
    .orderBy("monthly_fee_per_mobile")
)
