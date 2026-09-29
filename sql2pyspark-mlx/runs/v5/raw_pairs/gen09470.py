from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("is_flagged", "id", "attr_key")
w2 = Window.partitionBy("level").orderBy("is_flagged", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key").orderBy("is_flagged", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy(F.col("first_known_status_code_high").desc_nulls_last(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

flattened_attrs = (
    lines
    .filter(F.col("attr_key") != "version")
    .select(
        "id",
        "level",
        "status_code",
        "attr_key",
        "attr_value",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("is_flagged"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

valid = (
    flattened_attrs
    .filter(F.col("attr_key").isNotNull())
    .select(
        "*",
        F.length("level").alias("level_length"),
        F.round("is_flagged", 1).alias("is_flagged_rounded"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .filter(F.col("status_code_high").isNotNull())
    .select(
        "*",
        F.sum("is_flagged_rounded").over(w2).alias("cumulative_is_flagged_rounded"),
        F.first("status_code_high", ignorenulls=True).over(w3).alias("first_known_status_code_high"),
    )
)

cleaned = (
    metrics
    .filter(~F.col("attr_key").eqNullSafe(F.lit("version")))
    .withColumn("status_code_num", F.col("status_code").cast("string"))
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

summary = (
    latest
    .groupBy("attr_key")
    .agg(
        F.round(F.avg("is_flagged_rounded"), 2).alias("avg_is_flagged_rounded"),
        F.percentile_approx("cumulative_is_flagged_rounded", 0.95).alias("cumulative_is_flagged_rounded_p95"),
        F.percentile_approx("is_flagged", 0.5).alias("is_flagged_p50"),
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.avg("status_code_high"), 2).alias("avg_status_code_high"),
    )
)

result = (
    summary
    .select(
        "attr_key",
        "is_flagged_p50",
        "distinct_levels",
        "avg_status_code_high",
        F.when(F.col("attr_key").rlike("^(version|trace)$"), "GROUP_A").when(F.col("attr_key").rlike("^(region)$"), "PRIORITY").otherwise("EXTENDED").alias("attr_key_zone"),
    )
)
