from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("island_id", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rank").asc_nulls_last(), "id")

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-01 15:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

with_windows = (
    sessions_web_events
    .withColumn("new_group_flag_avg_6_prior", F.avg("new_group_flag").over(w3))
    .withColumn("duration_sec_rank", F.rank().over(w4))
    .withColumn("duration_sec_filled_avg_90d", F.avg("duration_sec_filled").over(w5))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

wide = (
    latest
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "page_view"])
    .agg(F.max("duration_sec_rank").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select("page", "add_to_cart_value_sum", "page_view_n")
    .orderBy(F.col("add_to_cart_value_sum").desc_nulls_first())
)
