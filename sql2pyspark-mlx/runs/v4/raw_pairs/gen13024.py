from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2024-03-19"))) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
        F.year("started_at").alias("started_at_year"),
    )
)

result = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"), F.col("c.event_time"))
)
