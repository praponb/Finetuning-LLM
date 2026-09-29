from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").asc_nulls_first())
w_hist = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.orderBy(F.col("element").desc_nulls_last())
w_ordered = Window.orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_limits").alias("element"))
)

metrics = (
    exploded
    .filter(F.col("effective_from") > "2024-10-01")
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.dense_rank().over(w_hist).alias("element_dense_rank"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

with_windows = metrics.withColumn("element_dense_rank_2", F.dense_rank().over(w_ordered))

filtered = (
    with_windows
    .filter(F.col("element_rank").between(3, 6))
    .select(
        "effective_from",
        "element",
        "element_dense_rank",
        "element_percent_rank",
        "element_dense_rank_2",
        (F.col("element") - F.col("element_dense_rank")).alias("element_minus_element_dense_rank"),
    )
)

valid = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-04-05").cast("date")) & (F.col("element_dense_rank_2") > 5))
    .select(
        "effective_from",
        "element",
        F.round((F.col("element") * 100), 2).alias("adjusted_element"),
    )
)

valid_price_tiers = (
    valid
    .filter((F.col("effective_from") > F.lit("2024-04-01").cast("date")) & (F.col("adjusted_element") <= 59))
    .select("*", F.round("adjusted_element", 2).alias("adjusted_element_rounded"))
)

result = (
    valid_price_tiers
    .select("effective_from", "adjusted_element_rounded")
    .orderBy(F.col("adjusted_element_rounded").desc_nulls_last())
)
