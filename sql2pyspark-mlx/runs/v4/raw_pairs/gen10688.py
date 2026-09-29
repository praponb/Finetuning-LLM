from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("is_new_session", "id")

enriched = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("b"), F.col("e.session_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.browser"))
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

ranked = (
    islands
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "HIGH").otherwise("LOW").alias("is_new_session_tier"),
        "page",
        F.when(F.col("duration_sec") >= 432, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("is_new_session")).otherwise(0)).alias("click_is_new_session"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/product", F.col("burst_id")).otherwise(0)).alias("product_burst_id"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.max(F.when(F.col("country") == "DE", F.col("burst_id"))).alias("de_max_burst_id"),
    )
    .orderBy(F.col("duration_sec_tier").desc_nulls_first())
)
