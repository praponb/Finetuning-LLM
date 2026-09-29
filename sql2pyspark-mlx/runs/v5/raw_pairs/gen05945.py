from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("search_count").desc())

lines = (
    df_app_logs
    .select("id", "service", "status_code", F.explode_outer("context").alias("k", "v"))
)

flattened_attrs = (
    lines
    .select(
        "service",
        "status_code",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)

cleaned = (
    flattened_attrs
    .filter(F.col("status_code_high") != 2)
    .withColumn("service_length", F.length("service"))
)

filtered = (
    cleaned
    .filter(F.col("status_code_high") == 1)
    .select("*", F.trim("service").alias("clean_service"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("status_code") >= 398, "HIGH").otherwise("LOW").alias("status_code_tier"),
        "service",
    )
    .agg(
        F.min("flag_bit2").alias("min_flag_bit2"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.percentile_approx("flag_bit2", 0.5).alias("flag_bit2_p50"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.avg("status_code").alias("avg_status_code"),
    )
    .withColumn("search_count_rank", F.rank().over(w))
    .orderBy(F.col("avg_status_code").desc_nulls_first())
)
