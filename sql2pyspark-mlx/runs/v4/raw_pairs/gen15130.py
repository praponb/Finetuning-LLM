from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id")
w3 = Window.partitionBy("country", "device").orderBy(F.col("is_desktop").desc(), "id")

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-03-24").cast("date")) & F.col("browser").isNotNull())
    .withColumn("is_desktop", F.when(F.col("device") == "desktop", 1).otherwise(0))
    .withColumn("browser_group", F.when(F.col("browser").isin(["Safari", "Edge"]), "Tier 1").otherwise("Other"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("UNKNOWN")))
)

scored = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("prev_is_desktop", F.lag("is_desktop", 2).over(w2))
    .withColumn("is_desktop_bucket", F.ntile(10).over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

totals = (
    eligible
    .groupBy(
        F.when(F.col("prev_is_desktop") >= 69, "HIGH").otherwise("LOW").alias("prev_is_desktop_tier"),
    )
    .agg(
        F.count_if(F.col("device") == "mobile").alias("mobile_events"),
        F.avg("seq_no").alias("avg_seq_no"),
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_rows"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
    )
)

result = (
    totals
    .select(
        "prev_is_desktop_tier",
        "mobile_events",
        "avg_seq_no",
        "firefox_rows",
        "de_count",
        "firefox_events",
    )
    .orderBy("prev_is_desktop_tier")
)
