from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("element").desc())
w_grp = Window.orderBy(F.col("element").asc_nulls_first())
w5 = Window.orderBy(F.col("element").asc_nulls_last())
w6 = Window.partitionBy("currency", "plan")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

enriched = (
    lines
    .withColumn("element_cume_dist", F.cume_dist().over(w_ordered))
    .withColumn("plan_max_element", F.max("element").over(w2))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("element_rank", F.rank().over(w_grp))
    .withColumn("element_cume_dist_3", F.cume_dist().over(w5))
)

filtered = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-03-01").cast("date"))
    .select(
        "*",
        F.least("element_cume_dist", "plan_max_element").alias("least_element_cume_dist_plan_max_element"),
        (F.col("element_rank") + F.col("element_cume_dist_3")).alias("element_rank_plus_element_cume_dist_3"),
    )
)

with_windows = (
    filtered
    .withColumn("currency_min_element_cume_dist_3", F.min("element_cume_dist_3").over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "plan_max_element",
        "element_cume_dist_2",
        "element_rank",
        "element_cume_dist_3",
        "least_element_cume_dist_plan_max_element",
    )
)
