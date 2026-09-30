from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy("flag_bit8")

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

flattened_attrs = (
    flattened
    .filter(F.col("status_code") > 350)
    .select(
        "id",
        "service",
        "status_code",
        "k",
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
        F.shiftright("status_code", 2).alias("status_code_high"),
    )
)

latest = (
    flattened_attrs
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("k")
    .agg(
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_count"),
        F.percentile_approx("flag_bit8", [0.25, 0.5, 0.75]).alias("flag_bit8_quartiles"),
        F.round(F.avg("flag_bit8"), 2).alias("avg_flag_bit8"),
        F.sum(F.when(F.col("k") == "region", F.col("status_code_high")).otherwise(0)).alias("region_status_code_high"),
    )
    .orderBy(F.col("region_status_code_high").desc())
)
