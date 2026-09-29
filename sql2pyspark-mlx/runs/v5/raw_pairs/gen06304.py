from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy("latency_ms", "id", "attr_key")
w2 = Window.partitionBy("attr_key", "host").orderBy(F.col("latency_ms").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key")
w4 = Window.partitionBy("host").orderBy("row_seq", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("attr_key").orderBy(F.col("prev_latency_ms_share").desc_nulls_first(), "id", "attr_key")

exploded = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode("context").alias("attr_key", "v"))
)

cleaned = (
    exploded
    .filter(~F.col("attr_key").isin(["trace", "region"]))
    .withColumn("clean_host", F.trim("host"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("N/A")))
)

metrics = (
    cleaned
    .select(
        "id",
        "host",
        "attr_key",
        F.lag("latency_ms", 1).over(w).alias("prev_latency_ms"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.round((F.col("prev_latency_ms") / F.sum("prev_latency_ms").over(w3)), 4).alias("prev_latency_ms_share"),
        F.sum("row_seq").over(w4).alias("running_row_seq"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("running_row_seq") >= 38, "PREMIUM").otherwise("STANDARD").alias("running_row_seq_tier"),
        F.when(F.col("prev_latency_ms_share") >= 0.74, "PREMIUM").otherwise("STANDARD").alias("prev_latency_ms_share_tier"),
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
)
