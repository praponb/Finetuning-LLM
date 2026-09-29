from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("browser")
w5 = Window.partitionBy("customer_id", "country").orderBy("started_at", "id")

metrics = (
    df_sessions
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w_running))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_running))
    .withColumn("row_seq", F.row_number().over(w3))
)

scored = (
    metrics
    .select(
        "*",
        F.min("row_seq").over(w4).alias("browser_min_row_seq"),
        F.lag("row_seq", 2, 0).over(w5).alias("prev_row_seq"),
        F.sum("row_seq").over(w4).alias("browser_sum_row_seq"),
    )
)

base = (
    scored
    .filter(F.col("browser").isNotNull() & (F.to_date("started_at") >= "2025-04-06"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "last_known_browser",
        "first_known_country",
        "last_known_country",
        "row_seq",
        "browser_min_row_seq",
        "prev_row_seq",
        "browser_sum_row_seq",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

by_type = (
    base
    .groupBy("customer_id")
    .pivot("device", ["mobile", "desktop"])
    .agg(F.avg("days_since_started_at").alias("amount"))
)

result = (
    by_type
    .select("customer_id", "desktop")
    .orderBy(F.col("customer_id").desc())
)
