from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last(), "id")
w_grp = Window.partitionBy("browser").orderBy("is_jp", "id")
w_ordered = Window.partitionBy("session_id").orderBy("is_jp", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("device", "session_id").orderBy("is_jp", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("browser").orderBy("is_jp", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.partitionBy("device").orderBy("prev_is_jp", "id")
w_recent = Window.partitionBy("browser").orderBy(F.col("prev_is_jp").desc(), "id")

ranked = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "session_id", "event_type", "duration_sec")
)

with_dims = (
    ranked
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.country").isin(["TH", "DE", "US"]))
    .select(
        "s.id",
        "s.session_id",
        F.col("b.browser"),
        F.col("b.country"),
        F.col("b.device"),
    )
)

base = (
    with_dims
    .filter(F.col("country") == "SG")
    .select(
        "id",
        "session_id",
        "browser",
        "device",
        F.when(F.col("country") == "JP", 1).otherwise(0).alias("is_jp"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

scored = (
    base
    .select(
        "*",
        F.lag("is_jp", 1).over(w_grp).alias("prev_is_jp"),
        F.avg("is_jp").over(w_ordered).alias("is_jp_avg_3_prior"),
        F.avg("is_jp").over(w4).alias("is_jp_avg_5_prior"),
        F.avg("is_jp").over(w5).alias("is_jp_avg_2"),
    )
)

metrics = (
    scored
    .withColumn("next_is_jp", F.lead("is_jp").over(w_running))
    .withColumn("next_is_jp_avg_3_prior", F.lead("is_jp_avg_3_prior").over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
    .withColumn("prev_is_jp_bucket", F.ntile(4).over(w_recent))
)

result = (
    metrics
    .select(
        "id",
        "session_id",
        "is_jp",
        "country_clean",
        "is_jp_avg_3_prior",
        "is_jp_avg_2",
        "next_is_jp",
        "next_is_jp_avg_3_prior",
        "row_seq",
    )
    .orderBy(F.col("session_id").desc())
)
