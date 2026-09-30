from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Firefox", 1)).alias("firefox_count"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_start_flag_per_edge"),
    )
    .orderBy(F.col("firefox_count").desc_nulls_first())
)
