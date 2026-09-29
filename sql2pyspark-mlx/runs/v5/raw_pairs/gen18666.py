from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("duration_sec_tier").orderBy(F.col("num_tiers").desc())

flattened_attrs = (
    df_app_logs
    .filter(~F.col("level").isin(["DEBUG", "WARN", "ERROR"]))
    .select(
        "id",
        "level",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        (F.col("status_code").bitwiseAND(4) != 0).cast("int").alias("flag_bit4"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("duration_sec") >= 3.3, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.when(F.col("num_context") >= 60, "HIGH").otherwise("LOW").alias("num_context_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.size(F.collect_set("tier")).alias("num_tiers"),
        F.round(F.avg("num_context"), 2).alias("avg_num_context"),
    )
    .withColumn("num_tiers_rank", F.rank().over(w))
    .orderBy(F.col("avg_num_context").desc())
)
