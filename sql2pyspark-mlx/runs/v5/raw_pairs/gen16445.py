from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy("browser")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.round(F.sum("island_id") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("island_id_per_firefox"),
    )
)
