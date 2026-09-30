from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("country") == "SG")
    .withColumn("device_upper", F.upper("device"))
    .withColumn("started_at_hour", F.hour("started_at"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("N/A")))
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupingSets([["browser", "device"], ["browser"]], "browser", "device")
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.when(F.col("country") == "TH", F.col("session_no"))).alias("th_max_session_no"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("new_group_flag_per_jp"),
    )
)
