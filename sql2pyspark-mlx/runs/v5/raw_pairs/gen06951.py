from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val_scaled").desc())
w2 = Window.partitionBy("plan").orderBy(F.col("val_scaled").desc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

valid = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-04-17").cast("date"))
    .withColumn("val_tier", F.when(F.col("val") >= 67, "High").when(F.col("val") >= 31, "Medium").otherwise("Low"))
)

base = (
    valid
    .filter((F.col("effective_from") >= F.lit("2024-05-31").cast("date")) & F.col("effective_from").between("2023-08-07", "2025-05-01"))
    .select(
        "plan",
        "effective_from",
        "val_tier",
        F.round((F.col("val") * 1.07), 2).alias("val_scaled"),
        F.ceil("val").alias("val_whole"),
    )
)

with_windows = (
    base
    .select(
        "effective_from",
        "val_scaled",
        "val_whole",
        F.dense_rank().over(w).alias("val_scaled_dense_rank"),
        F.cume_dist().over(w2).alias("val_scaled_cume_dist"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("val_whole") >= 88, "PREMIUM").otherwise("STANDARD").alias("val_whole_tier"),
        F.when(F.col("val_scaled") >= 75, "LARGE").otherwise("SMALL").alias("val_scaled_tier"),
    )
    .agg(
        F.percentile_approx("val_whole", 0.95).alias("val_whole_p95"),
        F.percentile_approx("val_scaled_cume_dist", [0.25, 0.5, 0.75]).alias("val_scaled_cume_dist_quartiles"),
        F.max("val_whole").alias("max_val_whole"),
    )
)

result = (
    summary
    .select(
        "val_whole_tier",
        "val_scaled_tier",
        "val_scaled_cume_dist_quartiles",
        "max_val_whole",
    )
    .orderBy(F.col("val_whole_tier").asc_nulls_last(), F.col("val_scaled_tier").desc())
)
