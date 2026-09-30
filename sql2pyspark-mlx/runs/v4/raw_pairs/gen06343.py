from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("started_at", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("browser").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.sum("balance").alias("sum_account_balance"))

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

with_gaps = (
    scored
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w4))

enriched = (
    sessionized
    .select(
        "*",
        F.avg("session_id").over(w_recent).alias("session_id_avg_6"),
        F.sum("event_seq").over(w_hist).alias("running_event_seq"),
        F.first("event_seq", ignorenulls=True).over(w7).alias("first_known_event_seq"),
    )
)

cleaned = (
    enriched
    .filter(F.col("event_seq") < 4)
    .select(
        "*",
        (F.col("running_event_seq") * F.col("session_start_flag")).alias("running_event_seq_times_session_start_flag"),
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_clean"),
    )
)

result = (
    cleaned
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "seq_no",
        "event_seq",
        "session_start_flag",
        "session_id",
        "running_event_seq",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("device_zone"),
        F.col("sum_account_balance"),
    )
    .orderBy("seq_no")
)
