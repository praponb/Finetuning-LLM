from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-09-08 20:00:00").cast("timestamp")) & (F.col("device") == "desktop"))
    .withColumn("clean_browser", F.trim("browser"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("N/A")))
)

with_prev = base.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("is_new_session_tier")
    .agg(
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("device") == "tablet", 1)), F.lit(0)), 2).alias("is_new_session_per_tablet"),
        F.min("started_at").alias("started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.grouping("is_new_session_tier").alias("gid"),
    )
    .orderBy(F.col("de_count").desc())
)
