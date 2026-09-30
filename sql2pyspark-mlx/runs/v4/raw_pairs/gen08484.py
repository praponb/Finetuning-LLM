from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device", "browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.lower("browser").alias("browser_lower"),
        F.coalesce("country", F.lit("OTHER")).alias("country_filled"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w2).alias("last_known_browser"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.last("browser", ignorenulls=True).over(w_recent).alias("last_known_browser_2"),
    )
)

result = (
    enriched
    .groupBy("browser")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min_2"),
    )
    .orderBy("event_count")
)
