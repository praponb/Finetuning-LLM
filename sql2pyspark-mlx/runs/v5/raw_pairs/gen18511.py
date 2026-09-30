from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "service").orderBy(F.col("is_search").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

kept_keys = (
    exploded
    .select("level", "service")
    .subtract(
        df_app_logs
        .filter(F.col("status_code") < 296)
        .select("level", "service")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.service") == F.col("k.service")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(~F.col("key").eqNullSafe(F.lit("region")))
    .select(
        "id",
        "service",
        "level",
        "key",
        "attr_value",
        F.when(F.col("service") == "search", 1).otherwise(0).alias("is_search"),
    )
)

metrics = (
    filtered
    .filter(F.col("level").isin(["INFO", "WARN", "ERROR"]))
    .select(
        "service",
        "level",
        "key",
        "attr_value",
        "is_search",
        F.percent_rank().over(w).alias("is_search_percent_rank"),
    )
)

result = (
    metrics
    .groupBy("service")
    .agg(
        F.percentile_approx("is_search_percent_rank", [0.25, 0.5, 0.75]).alias("is_search_percent_rank_quartiles"),
        F.percentile_approx("is_search_percent_rank", [0.25, 0.5, 0.75]).alias("is_search_percent_rank_quartiles_2"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
    .orderBy(F.col("num_keys").desc_nulls_last())
)
