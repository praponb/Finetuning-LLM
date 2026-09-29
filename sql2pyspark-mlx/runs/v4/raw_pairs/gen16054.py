from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("latency_ms", "id", "k")
w2 = Window.partitionBy("k")
w3 = Window.partitionBy("k").orderBy(F.col("status_code").desc())
w4 = Window.partitionBy("k").orderBy("next_latency_ms", "id", "k")

exploded = (
    df_app_logs
    .select("id", "latency_ms", "status_code", F.explode("context").alias("k", "value"))
)

metrics = (
    exploded
    .withColumn("next_latency_ms", F.lead("latency_ms", 1).over(w))
    .withColumn("corr_status_code_latency_ms", F.corr("status_code", "latency_ms").over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

grouped = (
    latest
    .groupBy("k")
    .agg(
        F.sum(F.when(F.col("k") == "trace", F.col("next_latency_ms")).otherwise(0)).alias("trace_next_latency_ms"),
        F.max("status_code_cume_dist").alias("max_status_code_cume_dist"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.size(F.collect_set("k")).alias("num_ks"),
    )
)

result = (
    grouped
    .select(
        "k",
        "trace_next_latency_ms",
        "latency_ms_bit_and",
        "num_ks",
        F.when(F.col("k").rlike("^(version|region)$"), "PRIORITY").when(F.col("k").rlike("^(trace)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("k_zone"),
    )
    .orderBy(F.col("trace_next_latency_ms").desc_nulls_last(), F.col("k").desc())
)
