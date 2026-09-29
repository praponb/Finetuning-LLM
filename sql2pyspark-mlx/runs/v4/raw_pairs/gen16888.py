from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

valid = (
    df_sessions
    .filter(F.to_date("started_at") < "2024-07-01")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

remaining_keys = (
    islands
    .select("id", "customer_id")
    .subtract(
        df_sessions
        .filter(F.col("device").isin(["tablet", "mobile"]))
        .select("id", "customer_id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_semi")
)

result = (
    kept
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "is_new_session",
        "island_id",
        F.when(F.col("device").rlike("^(desktop|tablet)$"), "OTHER_ZONE").when(F.col("device").rlike("^(mobile)$"), "CORE").otherwise("GROUP_A").alias("device_zone"),
        F.col("avg_order_amount"),
    )
)
