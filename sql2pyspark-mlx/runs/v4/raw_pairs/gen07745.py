from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 7, "HIGH").otherwise("LOW").alias("island_id_tier"),
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
    )
    .agg(
        F.max("prev_started_at").alias("last_seen"),
        F.max("started_at").alias("last_seen_2"),
        F.sum(F.when(F.col("browser") == "Chrome", F.col("island_id")).otherwise(0)).alias("chrome_island_id"),
        F.sum(F.when(F.col("browser") == "Edge", F.col("is_new_burst")).otherwise(0)).alias("edge_is_new_burst"),
        F.min("started_at").alias("first_started_at"),
        ((F.unix_timestamp(F.max("prev_started_at")) - F.unix_timestamp(F.min("prev_started_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("chrome_island_id") > 0)
    .orderBy(F.col("island_id_tier").desc_nulls_last())
)
