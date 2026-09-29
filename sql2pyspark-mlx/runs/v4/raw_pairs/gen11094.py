from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("page_length", F.length("page"))
    .withColumn("duration_sec_scaled", F.round((F.col("duration_sec") * 100), 2))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .cube("week_start")
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.round(F.sum("duration_sec_scaled") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_scaled_per_cart"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("page_length")).otherwise(0)).alias("checkout_page_length"),
        F.min("event_time").alias("first_seen"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.grouping("week_start").alias("gid"),
    )
)
