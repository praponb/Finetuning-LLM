from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.max("started_at").alias("last_seen"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
    )
)
