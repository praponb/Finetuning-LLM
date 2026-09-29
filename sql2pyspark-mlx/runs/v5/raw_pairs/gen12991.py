from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan").orderBy(F.col("element").desc())
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

with_windows = (
    flattened
    .select(
        "*",
        F.rank().over(w_recent).alias("element_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.dense_rank().over(w_recent).alias("element_dense_rank_2"),
        F.cume_dist().over(w_recent).alias("element_cume_dist"),
        F.sum("element").over(w3).alias("plan_sum_element"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-03-26").cast("date"))
    .select(
        "*",
        F.when(F.col("element") >= 72, "High").when(F.col("element") >= 23, "Medium").otherwise("Low").alias("element_band"),
    )
)

base = (
    cleaned
    .filter(F.col("element_cume_dist").between(0.34, 0.63))
    .withColumn("element_dense_rank_level", F.when(F.col("element_dense_rank") >= 8, "High").when(F.col("element_dense_rank") >= 2, "Medium").otherwise("Low"))
    .withColumn("element_whole", F.ceil("element"))
)

result = (
    base
    .withColumn("year_start", F.date_trunc("YEAR", "effective_from"))
    .groupingSets([["year_start", "plan"], ["year_start"], []], "year_start", "plan")
    .agg(
        F.avg("element_dense_rank_2").alias("avg_element_dense_rank_2"),
        F.sum("element_dense_rank").alias("total_element_dense_rank"),
        F.round(F.avg("plan_sum_element"), 2).alias("avg_plan_sum_element"),
        F.grouping_id().alias("gid"),
    )
)
