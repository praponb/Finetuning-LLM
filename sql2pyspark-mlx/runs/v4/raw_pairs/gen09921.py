from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.partitionBy("plan").orderBy(F.col("plan_length").desc_nulls_last())
w4 = Window.orderBy("element")

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

cleaned = (
    exploded
    .filter(F.col("element") >= 56)
    .withColumn("plan_length", F.length("plan"))
)

with_windows = (
    cleaned
    .withColumn("element_dense_rank", F.dense_rank().over(w))
    .withColumn("element_rank", F.rank().over(w2))
    .withColumn("plan_length_dense_rank", F.dense_rank().over(w3))
    .withColumn("element_cume_dist", F.cume_dist().over(w4))
)

cleaned_price_tiers = (
    with_windows
    .filter(F.col("element_dense_rank") > 8)
    .select(
        "element",
        "element_dense_rank",
        "element_rank",
        "plan_length_dense_rank",
        "element_cume_dist",
        F.col("element_dense_rank").cast("double").alias("element_dense_rank_num"),
    )
)

filtered = (
    cleaned_price_tiers
    .filter(F.col("element_dense_rank_num") >= 64)
    .select(
        "element",
        "element_dense_rank",
        "element_rank",
        "plan_length_dense_rank",
        "element_cume_dist",
        F.floor("element_dense_rank_num").alias("element_dense_rank_num_whole"),
    )
)

base = (
    filtered
    .filter(F.col("element") <= 80)
    .select(
        "*",
        F.when(F.col("element") >= 67, "High").when(F.col("element") >= 30, "Medium").otherwise("Low").alias("element_band"),
    )
)

result = (
    base
    .withColumn("element_tier", F.when(F.col("element") >= 61, "LARGE").otherwise("SMALL"))
    .withColumn("element_dense_rank_tier", F.when(F.col("element_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["element_tier", "element_dense_rank_tier"], ["element_tier"], []],
        "element_tier",
        "element_dense_rank_tier",
    )
    .agg(
        F.percentile_approx("element_dense_rank", [0.25, 0.5, 0.75]).alias("element_dense_rank_quartiles"),
        F.percentile_approx("element_rank", [0.25, 0.5, 0.75]).alias("element_rank_quartiles"),
        F.grouping("element_tier").alias("gid"),
    )
)
