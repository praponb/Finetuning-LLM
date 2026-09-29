from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_last())

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-05-27").cast("date"))
    .select(
        "*",
        F.when(F.col("browser").isin(["Safari", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

deduped = (
    sessionized
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_filled",
        "latest_rank",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "OTHER_ZONE").when(F.col("device").rlike("^(desktop)$"), "GROUP_B").otherwise("CORE").alias("device_zone"),
    )
)
