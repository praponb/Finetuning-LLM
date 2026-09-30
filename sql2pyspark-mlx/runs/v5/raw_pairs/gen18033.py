from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "level", F.explode_outer("context").alias("k", "attr_value"))
)

valid = (
    exploded
    .filter(F.col("k") == "region")
    .withColumn("level_prefix", F.substring("level", 1, 3))
    .withColumn("k_filled", F.coalesce("k", F.lit("UNKNOWN")))
)

valid_app_logs = (
    valid
    .filter((F.col("k").eqNullSafe(F.lit("trace"))) & F.col("level").isin(["DEBUG", "ERROR", "WARN"]))
    .select(
        "id",
        "level",
        "k",
        "attr_value",
        F.lower("level").alias("level_lower"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_filled_2"),
    )
)

cleaned = (
    valid_app_logs
    .filter(F.col("level").isin(["ERROR", "DEBUG", "WARN"]))
    .select("id", "level", "level_lower", "k_filled_2", F.upper("k").alias("k_upper"))
)

valid_app_logs_2 = (
    cleaned
    .filter(F.col("level") == "WARN")
    .select("*", F.initcap(F.trim("level")).alias("level_title"))
)

result = (
    valid_app_logs_2
    .cube("level")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_count_2"),
        F.count_if(F.col("level") == "DEBUG").alias("debug_events"),
    )
    .filter(F.col("warn_count") < 1000)
    .orderBy(F.col("event_count").desc_nulls_first())
)
