from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy("burst_id", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device", "country").orderBy(F.col("new_group_flag").desc())
w7 = Window.partitionBy("country").orderBy(F.col("burst_id").desc())
w8 = Window.partitionBy("country").orderBy("started_at", "id")
w9 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device", "country").orderBy("burst_id_dense_rank", "id")
w12 = Window.orderBy(F.col("burst_id_2").desc(), "id")

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

metrics = (
    islands
    .withColumn("burst_id_sum_30d", F.sum("burst_id").over(w3))
    .withColumn("burst_id_avg_5", F.avg("burst_id").over(w4))
    .withColumn("cumulative_new_group_flag", F.sum("new_group_flag").over(w5))
    .withColumn("new_group_flag_rank", F.rank().over(w6))
    .withColumn("burst_id_dense_rank", F.dense_rank().over(w7))
)

flagged_2 = (
    metrics
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w8) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_sessions = (
    flagged_2
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        "new_group_flag",
        "burst_id",
        "cumulative_new_group_flag",
        "burst_id_dense_rank",
        F.sum("is_new_burst").over(w9).alias("burst_id_2"),
    )
)

enriched = (
    islands_sessions
    .select(
        "id",
        "browser",
        "started_at",
        "cumulative_new_group_flag",
        "burst_id_dense_rank",
        F.first("browser", ignorenulls=True).over(w10).alias("first_known_browser"),
        F.lead("new_group_flag", 1).over(w11).alias("next_new_group_flag"),
        F.ntile(5).over(w12).alias("burst_id_2_bucket"),
    )
)

cleaned = (
    enriched
    .filter((F.col("started_at") > F.lit("2023-05-05 20:00:00").cast("timestamp")) & F.hour("started_at").between(7, 10))
    .select(
        "id",
        "browser",
        "cumulative_new_group_flag",
        "burst_id_dense_rank",
        "next_new_group_flag",
        F.least("cumulative_new_group_flag", "next_new_group_flag").alias("least_cumulative_new_group_flag_next_new_group_flag"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "browser",
        "cumulative_new_group_flag",
        F.when(F.col("browser").rlike("^(Edge|Firefox)$"), "GROUP_A").otherwise("PRIORITY").alias("browser_zone"),
    )
)
