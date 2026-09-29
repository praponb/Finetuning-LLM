from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.orderBy(F.col("element").desc())
w4 = Window.orderBy(F.col("element_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("element_rank_2").asc_nulls_first())
w6 = Window.orderBy(F.col("element_rank_2").desc_nulls_first())

flattened = df_price_tiers.select("id", F.explode_outer("discounts").alias("element"))

metrics = (
    flattened
    .withColumn("element_rank", F.rank().over(w))
    .withColumn("element_rank_2", F.rank().over(w2))
)

filtered = (
    metrics
    .filter(F.col("element_rank_2") != 6)
    .withColumn("element_rank_2_minus_element_rank", (F.col("element_rank_2") - F.col("element_rank")))
)

with_windows = (
    filtered
    .withColumn("element_rank_3", F.rank().over(w2))
    .withColumn("element_dense_rank", F.dense_rank().over(w3))
)

enriched = (
    with_windows
    .withColumn("element_rank_cume_dist", F.cume_dist().over(w4))
    .withColumn("element_rank_2_rank", F.rank().over(w5))
    .withColumn("element_rank_2_cume_dist", F.cume_dist().over(w6))
)

valid = (
    enriched
    .filter(F.col("element").isNull())
    .withColumn("element_rank_2_minus_element_rank_minus_element_rank_2_cume_dist", (F.col("element_rank_2_minus_element_rank") - F.col("element_rank_2_cume_dist")))
)

result = (
    valid
    .select(
        "id",
        "element",
        "element_rank",
        "element_rank_2",
        "element_rank_2_minus_element_rank",
        "element_rank_3",
        "element_dense_rank",
        "element_rank_cume_dist",
        "element_rank_2_rank",
    )
    .orderBy("element_rank")
)
