from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((~F.col("browser").eqNullSafe(F.lit("Edge"))) & F.col("customer_id").isNotNull())
    .select("*", F.lower("browser").alias("browser_lower"))
)

with_prev = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.countDistinct("country").alias("distinct_countries"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_rows"),
    )
)
