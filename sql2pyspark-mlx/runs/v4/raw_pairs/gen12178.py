from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("debug_rows").desc())

flattened = (
    df_app_logs
    .select("id", "service", "level", F.explode("context").alias("k", "attr_value"))
)

base = (
    flattened
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.upper("k").alias("k_upper"),
        F.coalesce("k", F.lit("N/A")).alias("k_clean"),
    )
)

filtered = (
    base
    .filter(F.col("level").isin(["DEBUG", "WARN"]))
    .select(
        "id",
        "service",
        "level",
        "k",
        "k_clean",
        F.concat("k", F.lit("/"), "level").alias("k_level"),
    )
)

result = (
    filtered
    .groupBy("k", "level")
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.countDistinct("level").alias("distinct_levels"),
    )
    .withColumn("debug_rows_rank", F.rank().over(w))
)
