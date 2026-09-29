from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("status_code").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("k", "level").orderBy("status_code", "id", "k").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("k").orderBy("log_ts", "id", "k")
w5 = Window.partitionBy("k").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

deduped = (
    flattened
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.last("k", ignorenulls=True).over(w2).alias("last_known_k"),
        F.avg("status_code").over(w3).alias("status_code_avg_5"),
    )
)

flagged = (
    enriched
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w4) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w5))

result = (
    islands
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "k",
        "v",
        "new_group_flag",
        "island_id",
        F.when(F.col("level").rlike("^(INFO|WARN)$"), "CORE").when(F.col("level").rlike("^(ERROR|DEBUG)$"), "GROUP_A").otherwise("EXTENDED").alias("level_zone"),
    )
    .orderBy(F.col("new_group_flag").desc_nulls_first())
)
