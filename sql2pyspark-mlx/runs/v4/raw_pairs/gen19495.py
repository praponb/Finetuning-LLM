from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_last(), "id")
w5 = Window.partitionBy("country").orderBy("started_at", "id")
w6 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_sessions
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.first("browser", ignorenulls=True).over(w_part).alias("first_known_browser"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w5).isNull() | (F.col("started_at") > F.lag("started_at").over(w5) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w6))

with_prev = islands.withColumn("prev_started_at", F.lag("started_at").over(w5))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w6))

result = (
    sessionized
    .select(
        "*",
        F.avg("island_id").over(w7).alias("island_id_avg_90d"),
        F.sum("island_id").over(w8).alias("cumulative_island_id"),
    )
)
