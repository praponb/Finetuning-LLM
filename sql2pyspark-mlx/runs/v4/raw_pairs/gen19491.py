from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at_date").desc(), "id")
w_hist = Window.partitionBy("browser", "country").orderBy(F.col("started_at_date").desc(), "id")
w_recent = Window.partitionBy("browser").orderBy("started_at_date", "id")
w4 = Window.orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device")
w6 = Window.partitionBy("customer_id", "device").orderBy("started_at_date", "id")
w7 = Window.orderBy("row_seq_2", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter(F.col("customer_id").isin([38, 39]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.date_format("started_at", "yyyy-MM-dd").alias("started_at_label"),
        F.to_date("started_at").alias("started_at_date"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w_hist).alias("row_seq_2"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at_date")), "left_anti")
)

with_windows_sessions = (
    kept
    .select(
        "*",
        F.sum("event_seq").over(w4).alias("running_event_seq"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w5)), 4).alias("event_seq_share"),
        F.lag("row_seq_2", 1).over(w6).alias("prev_row_seq_2"),
        F.ntile(4).over(w7).alias("row_seq_2_bucket"),
    )
)

result = (
    with_windows_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "country",
        "started_at_label",
        "row_seq_2",
        "event_seq",
        "running_event_seq",
        "event_seq_share",
        "prev_row_seq_2",
        F.when(F.col("country").rlike("^(UK|SG)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("country_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy("row_seq_2", "running_event_seq")
)
