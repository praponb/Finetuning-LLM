from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy("device").orderBy("started_at", "id")
w6 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-03-08 10:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .withColumn("country_filled", F.coalesce("country", F.lit("N/A")))
)

eligible = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

enriched = (
    eligible
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w).alias("last_known_country"),
        F.first("country", ignorenulls=True).over(w_grp).alias("first_known_country"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country_2"),
        F.first("browser", ignorenulls=True).over(w_grp).alias("first_known_browser"),
    )
)

enriched_sessions = (
    enriched
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

result = (
    enriched_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "country_filled",
        "first_known_country",
        "first_known_browser",
        "event_seq",
        F.when(F.col("country").rlike("^(JP|UK)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("CORE").alias("country_zone"),
    )
    .orderBy(F.col("id").desc(), "country")
)
