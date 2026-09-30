from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy("element_filled")
w4 = Window.partitionBy("currency").orderBy(F.col("element_filled").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

valid = (
    exploded
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-06-20")))
    .select(
        "*",
        F.coalesce("element", F.lit(0)).alias("element_filled"),
        F.when(F.col("element") >= 69, "High").when(F.col("element") >= 26, "Medium").otherwise("Low").alias("element_tier"),
    )
)

with_windows = (
    valid
    .filter(F.year("effective_from") == 2025)
    .select(
        "id",
        "element_filled",
        "element_tier",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.max("element").over(w2).alias("currency_max_element"),
        F.percent_rank().over(w3).alias("element_filled_percent_rank"),
        F.dense_rank().over(w4).alias("element_filled_dense_rank"),
    )
)

filtered = (
    with_windows
    .filter(F.col("currency_max_element") > 42)
    .withColumn("element_cume_dist_per_element_filled", (F.col("element_cume_dist") / F.nullif("element_filled", F.lit(0))))
)

summary = (
    filtered
    .rollup(
        F.when(F.col("element_filled") >= 83, "PREMIUM").otherwise("STANDARD").alias("element_filled_tier"),
        F.when(F.col("element_filled_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("element_filled_dense_rank_tier"),
    )
    .agg(
        F.percentile_approx("element_filled_percent_rank", [0.25, 0.5, 0.75]).alias("element_filled_percent_rank_quartiles"),
        F.sum("element_filled_percent_rank").alias("total_element_filled_percent_rank"),
        F.percentile_approx("element_filled", [0.25, 0.5, 0.75]).alias("element_filled_quartiles"),
        F.sum("element_cume_dist_per_element_filled").alias("total_element_cume_dist_per_element_filled"),
        F.count("*").alias("event_count"),
    )
)

result = (
    summary
    .select(
        "element_filled_tier",
        "element_filled_dense_rank_tier",
        "element_filled_percent_rank_quartiles",
        "total_element_cume_dist_per_element_filled",
        "event_count",
    )
)
