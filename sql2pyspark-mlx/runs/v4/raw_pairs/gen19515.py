from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser").orderBy("started_at", "id")
w_ordered = Window.partitionBy("country").orderBy(F.col("event_seq").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy("event_seq", "id")
w8 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_sessions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w2))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w3))
    .withColumn("first_known_country_2", F.first("country", ignorenulls=True).over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

filtered = (
    enriched
    .filter(F.col("customer_id").isNotNull() & (F.col("event_seq") > 5))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "row_seq",
        "last_known_browser",
        "first_known_country",
        "first_known_country_2",
        "event_seq",
        F.initcap(F.trim("device")).alias("device_title"),
        F.hour("started_at").alias("started_at_hour"),
    )
)

enriched_sessions = (
    filtered
    .withColumn("event_seq_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("event_seq_bucket", F.ntile(4).over(w7))
)

gaps = (
    enriched_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w5).isNull() | (F.col("started_at") > F.lag("started_at").over(w5) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w8))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        F.percentile_approx("event_seq_bucket", [0.25, 0.5, 0.75]).alias("event_seq_bucket_quartiles"),
        F.min("event_seq").alias("min_event_seq"),
    )
    .orderBy(F.col("min_event_seq").asc_nulls_first())
)
