from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2024-03-01 16:00:00").cast("timestamp"))
    .withColumn("started_at_date", F.to_date("started_at"))
)

flagged = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_session_tier"),
        F.date_trunc("YEAR", "started_at").alias("year_start"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max(F.when(F.col("country") == "US", F.col("is_new_session"))).alias("us_max_is_new_session"),
    )
    .orderBy(F.col("year_start").desc())
)
