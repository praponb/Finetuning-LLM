from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("year_start", F.date_trunc("YEAR", "started_at"))
    .groupingSets(
        [["browser", "session_id", "year_start"], ["browser", "session_id"], ["browser"]],
        "browser",
        "session_id",
        "year_start",
    )
    .agg(
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("browser") == "Chrome", 1)), F.lit(0)), 2).alias("new_group_flag_per_chrome"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("new_group_flag_per_firefox"),
    )
)
