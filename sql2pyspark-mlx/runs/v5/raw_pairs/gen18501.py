from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("monthly_fee", "id", "subscription_id")
w4 = Window.orderBy("credit_limit", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("row_seq", "id", "subscription_id").rowsBetween(-5, -1)

scored = (
    df_sessions
    .filter(F.col("country") == "SG")
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("country", ignorenulls=True).over(w2).alias("last_known_country"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_semi")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.credit_limit"), F.col("p.email"), F.col("p.signup_date"))
)

matched = (
    enriched
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored_sessions = (
    matched
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "last_known_country",
        "signup_date",
        "subscription_id",
        "monthly_fee",
        F.lead("row_seq", 2).over(w3).alias("next_row_seq"),
        F.last("browser", ignorenulls=True).over(w4).alias("last_known_browser"),
        F.avg("credit_limit").over(w_grp).alias("credit_limit_avg_5_prior"),
    )
)

result = (
    scored_sessions
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.segment").alias("segment"),
        F.col("c.country").alias("customer_country"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.subscription_id")).alias("total_subscription_id"),
    )
    .orderBy(F.col("segment").desc_nulls_first())
)
