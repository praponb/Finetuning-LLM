from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .withColumn("device_group", F.when(F.col("device").isin(["tablet", "desktop"]), "Group A").otherwise("Other"))
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "device_group",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("quarter_start", F.date_trunc("QUARTER", "started_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["burst_id", "quarter_start", "activity_month"], ["burst_id", "quarter_start"]],
        "burst_id",
        "quarter_start",
        "activity_month",
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.max("started_at").alias("last_started_at"),
        F.max(F.when(F.col("device") == "tablet", F.col("burst_id"))).alias("tablet_max_burst_id"),
    )
)
