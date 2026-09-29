from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("log_ts").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w3 = Window.partitionBy("level", "host").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k", "level").orderBy("status_code_cume_dist", "id", "k")
w5 = Window.partitionBy("host").orderBy("seq_no", "id", "k")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

enriched = (
    flattened
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w2))
    .withColumn("first_known_k", F.first("k", ignorenulls=True).over(w3))
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no_2"),
        F.lead("status_code_cume_dist", 2).over(w5).alias("next_status_code_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("log_ts") >= F.lit("2025-06-01").cast("date"))
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "k",
        "seq_no",
        "status_code_cume_dist",
        "first_known_k",
        "seq_no_2",
        F.col("latency_ms").cast("string").alias("latency_ms_str"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("status_code_cume_dist") >= 0.65, "LARGE").otherwise("SMALL").alias("status_code_cume_dist_tier"),
    )
    .agg(
        F.sum("latency_ms").alias("total_latency_ms"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
    )
    .orderBy(F.col("status_code_cume_dist_tier").desc())
)
