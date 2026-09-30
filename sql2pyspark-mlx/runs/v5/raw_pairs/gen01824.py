from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("country").isNotNull() & (~F.col("country").eqNullSafe(F.lit("TH"))))
    .withColumn("country_device", F.concat("country", F.lit("/"), "device"))
    .withColumn("browser_clean", F.coalesce("browser", F.lit("NONE")))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "country_device",
        "browser_clean",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_sessions
    .groupBy(F.date_trunc("YEAR", "started_at").alias("year_start"))
    .agg(
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_no_per_edge"),
        F.sum("session_no").alias("total_session_no"),
    )
)
