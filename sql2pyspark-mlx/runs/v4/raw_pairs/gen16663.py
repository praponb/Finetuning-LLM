from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("plan_length").asc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("plan_length").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("element").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.lit("2024-10-03").cast("date"))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM-dd"))
    .withColumn("plan_length", F.length("plan"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.cume_dist().over(w).alias("plan_length_cume_dist"),
        F.rank().over(w2).alias("element_rank"),
        F.min("plan_length").over(w3).alias("plan_min_plan_length"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.dense_rank().over(w4).alias("plan_length_dense_rank"),
        F.cume_dist().over(w5).alias("element_cume_dist"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("plan_length_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("plan_length_dense_rank_tier"),
        F.when(F.col("plan_min_plan_length") >= 74, "HIGH").otherwise("LOW").alias("plan_min_plan_length_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("element_cume_dist", [0.25, 0.5, 0.75]).alias("element_cume_dist_quartiles"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("plan") == "enterprise", 1)), F.lit(0)), 2).alias("element_per_enterprise"),
    )
    .filter(F.col("cnt") > 1)
)

result = (
    totals
    .select(
        "plan_length_dense_rank_tier",
        "plan_min_plan_length_tier",
        "cnt",
        "element_cume_dist_quartiles",
    )
    .orderBy(F.col("plan_min_plan_length_tier").asc_nulls_first())
)
