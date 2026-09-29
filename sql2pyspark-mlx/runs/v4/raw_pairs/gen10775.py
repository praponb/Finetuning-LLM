from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w_part = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_grp = Window.partitionBy("country", "browser").orderBy("web_event_page_length", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("browser", "country")
w10 = Window.orderBy("is_new_session", "id")

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w3).alias("first_known_browser"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("started_at") >= F.lit("2023-07-14").cast("date"))
    .select(
        "*",
        F.date_format("started_at", "yyyy-MM").alias("started_at_label"),
        F.col("seq_no").cast("string").alias("seq_no_num"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

filtered = (
    with_last
    .filter(F.col("country").isin(["TH", "JP"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "seq_no",
        "started_at_label",
        "seq_no_num",
        "web_event_page",
        F.length("web_event_page").alias("web_event_page_length"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w6) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w3))

result = (
    islands
    .select(
        "*",
        F.max("seq_no").over(w_part).alias("seq_no_max_7d"),
        F.avg("session_no").over(w_grp).alias("session_no_avg_6"),
        F.sum("is_new_session").over(w9).alias("browser_sum_is_new_session"),
        F.ntile(5).over(w10).alias("is_new_session_bucket"),
    )
)
