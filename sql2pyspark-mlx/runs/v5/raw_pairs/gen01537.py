from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("level").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("first_known_latency_ms").asc_nulls_first(), "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

ranked = (
    exploded
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "level", "latency_ms", "attr_key", "attr_value")
)

scored = (
    ranked
    .filter(~F.col("attr_key").isin(["region", "trace"]))
    .select(
        "id",
        "level",
        "attr_key",
        F.min("latency_ms").over(w2).alias("attr_key_min_latency_ms"),
        F.first("latency_ms", ignorenulls=True).over(w3).alias("first_known_latency_ms"),
    )
)

ranked_app_logs = (
    scored
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select("id", "attr_key")
)

result = (
    ranked_app_logs
    .rollup("attr_key")
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys_2"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
