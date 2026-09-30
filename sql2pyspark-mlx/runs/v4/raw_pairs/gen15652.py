from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_time"), F.col("c.duration_sec"))
)

result = (
    latest_match
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "event_time",
        F.when(F.col("browser").rlike("^(Safari|Firefox)$"), "EXTENDED").otherwise("GROUP_A").alias("browser_zone"),
    )
)
