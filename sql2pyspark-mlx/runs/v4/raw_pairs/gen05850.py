from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("session_id").desc(), "id")
w4 = Window.partitionBy("browser").orderBy("session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("session_id", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("browser").orderBy("session_id", "id")
w7 = Window.partitionBy("device", "country").orderBy(F.col("running_new_group_flag").desc(), "id")
w8 = Window.partitionBy("browser", "device").orderBy("cumulative_new_group_flag", "id")
w9 = Window.partitionBy("browser").orderBy(F.col("prev_new_group_flag").desc(), "id")
w10 = Window.partitionBy("browser")

flagged = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

enriched = (
    sessions_sessions
    .select(
        "*",
        F.ntile(10).over(w3).alias("session_id_bucket"),
        F.sum("new_group_flag").over(w2).alias("running_new_group_flag"),
        F.sum("new_group_flag").over(w4).alias("cumulative_new_group_flag"),
        F.avg("new_group_flag").over(w5).alias("new_group_flag_avg_6_prior"),
        F.lag("new_group_flag", 1, 0).over(w6).alias("prev_new_group_flag"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    ranked
    .withColumn("next_new_group_flag", F.lead("new_group_flag", 2).over(w8))
    .withColumn("prev_new_group_flag_bucket", F.ntile(4).over(w9))
    .withColumn("browser_avg_prev_new_group_flag", F.avg("prev_new_group_flag").over(w10))
)

result = (
    with_windows
    .select(
        "id",
        "device",
        "browser",
        "session_id",
        "running_new_group_flag",
        "cumulative_new_group_flag",
        "new_group_flag_avg_6_prior",
        "prev_new_group_flag",
        "next_new_group_flag",
    )
    .orderBy(F.col("device").desc_nulls_last(), F.col("browser").desc())
)
