from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.started_at",
        "s.country",
        F.col("c.duration_sec"),
        F.col("c.page"),
    )
)

result = (
    with_last
    .groupBy("page")
    .agg(
        F.min("started_at").alias("first_seen"),
        F.countDistinct("country").alias("distinct_countries"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("distinct_countries").desc())
)
