from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-10-01")
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 382, "High").when(F.col("duration_sec") >= 215, "Medium").otherwise("Low"))
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("page_clean", F.coalesce("page", F.lit("DIRECT")))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w3))

cleaned = (
    islands
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-12-24"))) & F.col("page").isNotNull() & (F.col("session_no") >= 7))
    .select(
        "*",
        F.when(F.col("page").isin(["/product", "/search"]), "Group A").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

by_type = (
    cleaned
    .groupBy("event_type")
    .pivot("page", ["/product", "/home"])
    .agg(F.sum("days_since_event_time").alias("total"))
    .select(
        "event_type",
        F.col("/product").alias("product"),
        F.col("/home").alias("home"),
    )
)

result = (
    by_type
    .select("event_type", "product")
    .orderBy(F.col("event_type").desc())
)
