from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "device").orderBy(F.col("started_at_period").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.date_trunc("year", "started_at").alias("started_at_period"),
        F.coalesce("browser", F.lit("N/A")).alias("resolved_browser"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

eligible = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"))
)

enriched_sessions = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country").alias("customer_country"), F.col("p.name"))
)

wide = (
    enriched_sessions
    .groupBy("customer_id")
    .pivot("device", ["tablet", "mobile", "desktop"])
    .agg(F.max("session_start_flag").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select(
        "customer_id",
        "tablet_value_sum",
        "tablet_n",
        "mobile_value_sum",
        "desktop_value_sum",
        "desktop_n",
    )
    .orderBy("desktop_n")
)
