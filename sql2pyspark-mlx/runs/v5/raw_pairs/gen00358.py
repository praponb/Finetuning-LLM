from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element_rank").desc_nulls_last())
w3 = Window.orderBy(F.col("element_rank").desc())
w4 = Window.orderBy(F.col("element_rank").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-05-06")))
    .withColumn("plan_upper", F.upper("plan"))
    .withColumn("plan_upper_2", F.upper("plan"))
)

scored = (
    filtered
    .select(
        "id",
        "effective_from",
        "plan_upper",
        F.rank().over(w).alias("element_rank"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w2).alias("element_rank_dense_rank"),
        F.rank().over(w3).alias("element_rank_rank"),
        F.percent_rank().over(w4).alias("element_rank_percent_rank"),
    )
)

filtered_price_tiers = (
    with_windows
    .filter(F.col("effective_from") > F.lit("2024-05-09").cast("date"))
    .withColumn("element_rank_percent_rank_per_element_rank_rank", F.round((F.col("element_rank_percent_rank") / F.col("element_rank_rank")), 2))
)

result = (
    filtered_price_tiers
    .select(
        "id",
        "effective_from",
        "plan_upper",
        "element_rank",
        "element_rank_percent_rank",
        "element_rank_percent_rank_per_element_rank_rank",
    )
    .orderBy(F.col("effective_from").desc_nulls_first())
)
