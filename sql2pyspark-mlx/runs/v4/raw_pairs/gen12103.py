from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id")

enriched = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w2))
    .withColumn("first_known_browser_2", F.first("browser", ignorenulls=True).over(w3))
)

with_windows = (
    enriched
    .withColumn("prev_row_seq", F.lag("row_seq", 2).over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

matched = (
    with_windows
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .rollup("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("row_seq"))).alias("enterprise_total_row_seq"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.min("started_at").alias("first_seen"),
        F.max(F.when(F.col("plan") == "premium", F.col("row_seq"))).alias("premium_max_row_seq"),
        F.round(F.avg("subscription_id"), 2).alias("avg_subscription_id"),
    )
    .orderBy("enterprise_total_row_seq")
)
