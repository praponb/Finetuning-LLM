from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("status_code", "id", "k")
w_grp_2 = Window.partitionBy("service", "k").orderBy("status_code", "id", "k")
w3 = Window.partitionBy("service", "k").orderBy(F.col("status_code").desc(), "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

metrics = (
    exploded
    .filter(F.col("k") == "version")
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "k",
        "value",
        F.lag("latency_ms", 1).over(w_grp).alias("prev_latency_ms"),
        F.ntile(4).over(w_grp_2).alias("status_code_bucket"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "service",
        "status_code",
        "k",
        "prev_latency_ms",
        "status_code_bucket",
    )
)

base = (
    latest
    .filter(F.col("status_code") > 263)
    .select(
        "id",
        "status_code",
        "k",
        F.when(F.col("status_code_bucket") >= 9, "High").when(F.col("status_code_bucket") >= 3, "Medium").otherwise("Low").alias("status_code_bucket_level"),
        F.when(F.col("k") == "region", 1).otherwise(0).alias("is_region"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

kept_keys = (
    base
    .select("id")
    .subtract(
        df_app_logs
        .filter(F.col("latency_ms") != 816)
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "status_code",
        "k",
        "status_code_bucket_level",
        "is_region",
        "k_filled",
        F.when(F.col("k").rlike("^(version|region)$"), "EXTENDED").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy("k_zone")
)
