from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at_period", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-12-07 02:00:00"))) & F.col("country").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "device", "browser").alias("device_browser_key"),
        F.date_trunc("month", "started_at").alias("started_at_period"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("started_at_period").over(w).isNull() | (F.col("started_at_period") > F.lag("started_at_period").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("device")
    .agg(
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_start_flag_per_edge"),
        F.max("session_no").alias("max_session_no"),
    )
    .orderBy(F.col("device").asc_nulls_first())
)
