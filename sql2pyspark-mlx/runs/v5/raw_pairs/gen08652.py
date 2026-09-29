from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("created_at_hour").asc_nulls_last())
w2 = Window.partitionBy("element")
w3 = Window.partitionBy("element").orderBy(F.col("created_at_hour_percent_rank").desc_nulls_last())
w4 = Window.partitionBy("element").orderBy(F.col("created_at_hour").desc_nulls_first())
w5 = Window.orderBy(F.col("created_at_hour_percent_rank").desc())
w6 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

flattened_carts = flattened.select("created_at", "v", F.explode("coupon_codes").alias("element"))

base = (
    flattened_carts
    .filter(F.col("element").eqNullSafe(F.lit("SPRING10")))
    .select(
        "*",
        F.when(F.col("element").isin(["BUNDLE", "VIP"]), "Tier 1").otherwise("Other").alias("element_group"),
        F.hour("created_at").alias("created_at_hour"),
        F.coalesce("element", F.lit("N/A")).alias("element_filled"),
    )
)

base_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull())
    .withColumn("element_title", F.initcap(F.trim("element")))
)

with_windows = (
    base_carts
    .select("*", F.percent_rank().over(w).alias("created_at_hour_percent_rank"))
)

metrics = (
    with_windows
    .select(
        "*",
        F.min("created_at_hour").over(w2).alias("element_min_created_at_hour"),
        F.percent_rank().over(w3).alias("created_at_hour_percent_rank_percent_rank"),
        F.dense_rank().over(w4).alias("created_at_hour_dense_rank"),
        F.rank().over(w5).alias("created_at_hour_percent_rank_rank"),
        F.sum("created_at_hour").over(w6).alias("created_at_hour_sum_15m"),
    )
)

result = (
    metrics
    .select(
        "created_at",
        "v",
        "element_group",
        "element_filled",
        "element_title",
        "created_at_hour_percent_rank",
        "element_min_created_at_hour",
        "created_at_hour_percent_rank_percent_rank",
        "created_at_hour_dense_rank",
    )
    .orderBy(F.col("created_at_hour_percent_rank_percent_rank").desc())
)
