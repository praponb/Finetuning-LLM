from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("prev_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

joined = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.id", "s.customer_id", "s.started_at", "s.country", F.col("c.name"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.page").isin(["/search", "/cart"])), "left_anti")
)

prev_events = not_flagged.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .filter(F.col("customer_id").isNotNull())
    .withColumn("country_avg_session_start_flag", F.avg("session_start_flag").over(w3))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w4))
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.session_start_flag",
        "s.session_no",
        "s.country_avg_session_start_flag",
        "s.first_known_country",
        F.col("c.page"),
    )
)

result = (
    with_latest
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .cube("session_no", "session_start_flag_tier")
    .agg(
        F.size(F.collect_set("page")).alias("num_pages"),
        F.sum(F.when(F.col("country") == "SG", F.col("session_start_flag"))).alias("sg_total_session_start_flag"),
        F.grouping_id().alias("grouping_level"),
    )
)
