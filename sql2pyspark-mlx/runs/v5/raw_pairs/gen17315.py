from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w4 = Window.orderBy(F.col("element_dense_rank").desc())
w5 = Window.orderBy(F.col("element_rank").asc_nulls_last())

exploded = df_price_tiers.select("id", "plan", F.explode_outer("tier_prices").alias("element"))

base = (
    exploded
    .filter(F.col("element") >= 75)
    .select(
        "*",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.when(F.col("plan").isin(["pro", "basic"]), "Tier 1").otherwise("Other").alias("plan_group"),
    )
)

enriched = (
    base
    .select(
        "id",
        "plan_title",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.min("element").over(w2).alias("plan_min_element"),
        F.rank().over(w3).alias("element_rank"),
    )
)

metrics = (
    enriched
    .filter(F.col("element_rank") == 4)
    .select("*", F.cume_dist().over(w4).alias("element_dense_rank_cume_dist"))
)

scored = metrics.withColumn("element_rank_cume_dist", F.cume_dist().over(w5))

stats = (
    scored
    .groupBy(
        F.when(F.col("plan_min_element") >= 81, "HIGH").otherwise("LOW").alias("plan_min_element_tier"),
    )
    .agg(
        F.percentile_approx("plan_min_element", [0.25, 0.5, 0.75]).alias("plan_min_element_quartiles"),
        F.sum("element_rank").alias("total_element_rank"),
        F.percentile_approx("element_dense_rank_cume_dist", 0.5).alias("element_dense_rank_cume_dist_p50"),
    )
)

result = (
    stats
    .select(
        "plan_min_element_tier",
        "total_element_rank",
        "element_dense_rank_cume_dist_p50",
    )
)
