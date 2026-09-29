from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("days_since_started_at").desc_nulls_first())
w4 = Window.partitionBy("browser").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("is_new_burst").desc(), "id")
w6 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("days_since_started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-09-06"))) & F.col("customer_id").isNotNull())
    .withColumn("days_since_started_at", F.datediff(F.current_date(), "started_at"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("UNKNOWN")))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

ranked = (
    islands
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("is_new_burst").over(w4).alias("running_sum_is_new_burst"),
        F.min("is_new_burst").over(w4).alias("running_min_is_new_burst"),
        F.max("is_new_burst").over(w4).alias("running_max_is_new_burst"),
        F.count("is_new_burst").over(w4).alias("running_count_is_new_burst"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .filter(F.col("is_new_burst") >= 1)
    .select(
        "*",
        F.sum("burst_id").over(w6).alias("running_burst_id"),
        F.ntile(4).over(w7).alias("days_since_started_at_bucket"),
    )
)
