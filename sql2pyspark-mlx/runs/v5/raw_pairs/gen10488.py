from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("burst_id", "id")
w4 = Window.orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country")
w_part = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(-6, -1)

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "device",
        "started_at",
        "country",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

with_windows = (
    islands
    .withColumn("next_burst_id", F.lead("burst_id").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

with_windows_sessions = (
    with_windows
    .withColumn("running_burst_id", F.sum("burst_id").over(w4))
    .withColumn("country_sum_next_burst_id", F.sum("next_burst_id").over(w5))
)

enriched = (
    with_windows_sessions
    .select(
        "id",
        "device",
        "country",
        "next_burst_id",
        "event_seq",
        F.last("running_burst_id", ignorenulls=True).over(w_part).alias("last_known_running_burst_id"),
        F.avg("country_sum_next_burst_id").over(w7).alias("country_sum_next_burst_id_avg_6_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "device",
        "country",
        "next_burst_id",
        "event_seq",
        "last_known_running_burst_id",
        "country_sum_next_burst_id_avg_6_prior",
    )
    .orderBy(
        F.col("event_seq").asc_nulls_first(),
        "country_sum_next_burst_id_avg_6_prior",
    )
)
