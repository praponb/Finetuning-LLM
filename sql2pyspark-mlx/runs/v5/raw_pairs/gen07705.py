from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_hist = Window.partitionBy("browser", "device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Chrome", "Safari"]))
    .withColumn("clean_country", F.trim("country"))
    .withColumn("device_country_key", F.concat_ws("/", "device", "country"))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

with_windows = (
    eligible
    .filter(F.col("customer_id").isin([13, 18, 21, 30]))
    .withColumn("first_known_country", F.first("country", ignorenulls=True).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("first_known_country_2", F.first("country", ignorenulls=True).over(w_hist))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.page").alias("web_event_page"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "browser",
        "country",
        "clean_country",
        "device_country_key",
        "first_known_country",
        "row_seq",
        "web_event_page",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "OTHER_ZONE").when(F.col("browser").rlike("^(Firefox|Edge)$"), "CORE").otherwise("PRIORITY").alias("browser_zone"),
    )
    .orderBy(F.col("row_seq").desc())
)
