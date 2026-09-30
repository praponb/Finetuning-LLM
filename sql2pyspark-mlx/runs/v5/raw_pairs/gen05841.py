from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy("element")
w_ordered = Window.orderBy(F.col("element_rank").desc_nulls_last())
w5 = Window.orderBy(F.col("element_rank").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode("tier_limits").alias("element"))

scored = (
    flattened
    .filter(F.col("element").between(36, 77))
    .select(
        "id",
        "element",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.rank().over(w2).alias("element_rank"),
    )
)

valid = (
    scored
    .filter(F.col("element") < 20)
    .select("*", F.col("element_rank").cast("double").alias("element_rank_str"))
)

metrics = (
    valid
    .select(
        "element",
        "element_percent_rank",
        F.rank().over(w3).alias("element_rank_2"),
        F.dense_rank().over(w_ordered).alias("element_rank_dense_rank"),
        F.rank().over(w5).alias("element_rank_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("element_percent_rank").between(0.55, 0.64))
    .select(
        "element_percent_rank",
        "element_rank_dense_rank",
        "element_rank_rank",
        (F.col("element") + F.col("element_rank_dense_rank")).alias("element_plus_element_rank_dense_rank"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("element_rank_rank") >= 9, "LARGE").otherwise("SMALL").alias("element_rank_rank_tier"),
        F.when(F.col("element_plus_element_rank_dense_rank") >= 90, "LARGE").otherwise("SMALL").alias("element_plus_element_rank_dense_rank_tier"),
    )
    .agg(
        F.round(F.avg("element_rank_dense_rank"), 2).alias("avg_element_rank_dense_rank"),
        F.avg("element_rank_dense_rank").alias("avg_element_rank_dense_rank_2"),
    )
    .orderBy(F.col("avg_element_rank_dense_rank_2").asc_nulls_last())
)
