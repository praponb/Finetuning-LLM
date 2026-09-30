from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w_grp = Window.orderBy(F.col("val_scaled").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

filtered = (
    lines
    .filter((F.col("effective_from") > F.lit("2024-04-24").cast("date")) & (F.col("effective_from") >= "2024-01-20"))
    .withColumn("val_scaled", F.round((F.col("val") * 0.9), 2))
)

remaining_keys = (
    filtered
    .select("currency", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("currency", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_price_tiers = (
    kept_rows
    .filter(F.col("effective_from") >= F.lit("2024-03-13").cast("date"))
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "val_scaled",
        F.date_sub("effective_from", 46).alias("effective_from_minus_46d"),
    )
)

base = (
    filtered_price_tiers
    .filter(F.col("val_scaled") >= 65)
    .select(
        "effective_from",
        "val",
        "val_scaled",
        "effective_from_minus_46d",
        F.round("val", 2).alias("val_rounded"),
    )
)

metrics = (
    base
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_scaled_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("val_scaled_dense_rank_2", F.dense_rank().over(w_grp))
)

base_price_tiers = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-03-14").cast("date"))
    .select("*", F.least("val", "val_rounded").alias("least_val_val_rounded"))
)

grouped = (
    base_price_tiers
    .groupBy(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.min("val_scaled_dense_rank").alias("min_val_scaled_dense_rank"),
        F.max("val_scaled_dense_rank_2").alias("max_val_scaled_dense_rank_2"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.round(F.avg("val_scaled_dense_rank"), 2).alias("avg_val_scaled_dense_rank"),
        F.percentile_approx("least_val_val_rounded", 0.95).alias("least_val_val_rounded_p95"),
        F.avg("val").alias("avg_val_2"),
    )
    .filter(F.col("min_val_scaled_dense_rank") < 1000000)
)

result = (
    grouped
    .select(
        "week_start",
        "min_val_scaled_dense_rank",
        "max_val_scaled_dense_rank_2",
        "avg_val",
        "avg_val_scaled_dense_rank",
        "least_val_val_rounded_p95",
    )
)
