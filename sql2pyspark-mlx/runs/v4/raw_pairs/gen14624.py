from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-05-27").cast("date"))
    .select("*", F.col("duration_sec").cast("string").alias("duration_sec_str"))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy("island_id")
    .agg(
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_rows"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
    )
    .orderBy(F.col("avg_session_start_flag").desc())
)
