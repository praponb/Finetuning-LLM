from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("is_auth", "id", "attr_key")
w_running = Window.partitionBy("attr_key", "service").orderBy("is_auth", "id", "attr_key")
w3 = Window.partitionBy("service").orderBy("is_auth", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "attr_key",
        F.initcap(F.trim("service")).alias("service_title"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("attr_key") == "region")
    .select(
        "id",
        "service",
        "level",
        "attr_key",
        F.upper("service").alias("service_upper"),
        F.when(F.col("service") == "auth", 1).otherwise(0).alias("is_auth"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("attr_key_filled_2"),
    )
)

scored = (
    cleaned
    .withColumn("prev_is_auth", F.lag("is_auth", 1).over(w))
    .withColumn("next_is_auth", F.lead("is_auth", 1).over(w_running))
    .withColumn("running_is_auth", F.sum("is_auth").over(w3))
)

grouped = (
    scored
    .groupBy("attr_key")
    .agg(
        F.percentile_approx("next_is_auth", [0.25, 0.5, 0.75]).alias("next_is_auth_quartiles"),
        F.max(F.when(F.col("service") == "auth", F.col("prev_is_auth"))).alias("auth_max_prev_is_auth"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select("attr_key", "auth_max_prev_is_auth", "row_count")
    .orderBy(F.col("auth_max_prev_is_auth").desc())
)
