from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("island_id_sum_1h", "id", "k")
w_running = Window.partitionBy("k")
w_ordered = Window.orderBy("island_id", "id", "k")
w7 = Window.partitionBy("k").orderBy("next_island_id", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "log_ts",
        "k",
        "value",
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

gaps = (
    parsed
    .withColumn("is_new_session", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select("id", "log_ts", "k", F.sum("is_new_session").over(w2).alias("island_id"))
)

with_windows = (
    islands
    .filter(F.col("island_id").between(5, 7))
    .select(
        "id",
        "k",
        "island_id",
        F.lead("island_id", 2, 0).over(w).alias("next_island_id"),
        F.sum("island_id").over(w_grp).alias("island_id_sum_1h"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "k",
        F.row_number().over(w4).alias("row_seq"),
        F.min("island_id_sum_1h").over(w_running).alias("k_min_island_id_sum_1h"),
        F.ntile(10).over(w_ordered).alias("island_id_bucket"),
        F.lead("island_id_sum_1h", 1).over(w7).alias("next_island_id_sum_1h"),
    )
)
