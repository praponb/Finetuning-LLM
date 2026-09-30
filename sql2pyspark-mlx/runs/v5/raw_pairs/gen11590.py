from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("status_code_high", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc(), "id", "key")
w3 = Window.partitionBy("key").orderBy("is_flagged", "id", "key").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy(F.col("status_code_high").desc(), "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

parsed = (
    exploded
    .select(
        "id",
        "status_code",
        "key",
        "v",
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

ranked = (
    parsed
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    ranked
    .filter(F.col("key").isNotNull())
    .select("*", F.round("status_code_high", 2).alias("status_code_high_rounded"))
)

ranked_app_logs = (
    base
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked_app_logs
    .select(
        "id",
        "status_code",
        "key",
        "status_code_high",
        "status_code_high_rounded",
        F.count("is_flagged").over(w3).alias("last3_count_is_flagged"),
        F.min("is_flagged").over(w3).alias("last3_min_is_flagged"),
        F.sum("is_flagged").over(w3).alias("last3_sum_is_flagged"),
        F.max("is_flagged").over(w3).alias("last3_max_is_flagged"),
        F.ntile(4).over(w4).alias("status_code_high_bucket"),
    )
)

stats = (
    enriched
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "region", 1)).alias("region_count"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("key") == "trace", 1)).alias("trace_count_2"),
    )
)

result = (
    stats
    .select(
        "key",
        "region_count",
        "key_list",
        "trace_count_2",
        F.when(F.col("key").rlike("^(trace|version)$"), "CORE").otherwise("PRIORITY").alias("key_zone"),
    )
    .orderBy(F.col("region_count").desc())
)
