from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("device").orderBy("started_at", "id")
w_hist = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.row_number().over(w_running).alias("event_seq_2"),
        F.first("browser", ignorenulls=True).over(w_hist).alias("first_known_browser"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("event_seq_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_2_tier"),
    )
    .agg(
        F.max(F.when(F.col("country") == "UK", F.col("event_seq"))).alias("uk_max_event_seq"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("started_at").alias("first_started_at"),
        F.max("started_at").alias("last_started_at"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
