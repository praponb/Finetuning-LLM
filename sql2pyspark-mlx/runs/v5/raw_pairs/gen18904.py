from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w_part_2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id")

metrics = (
    df_sessions
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_part))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w_part))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_part_2))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    latest
    .filter(F.col("browser").isNotNull())
    .select(
        "*",
        (F.col("row_seq") - F.col("event_seq")).alias("row_seq_minus_event_seq"),
        F.upper("country").alias("country_upper"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

attributed = (
    kept
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .select(
        "id",
        "subscription_id",
        "browser",
        F.col("row_seq_minus_event_seq").alias("ROW_SEQ_MINUS_EVENT_SEQ"),
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
    )
    .unpivot(
        ids=["id", "subscription_id", "browser"],
        values=["ROW_SEQ_MINUS_EVENT_SEQ", "MONTHLY_FEE_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
