from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode("discounts").alias("element"))
)

scored = exploded.withColumn("element_dense_rank", F.dense_rank().over(w))

base = (
    scored
    .filter(F.col("element") > 33)
    .withColumn("element_plus_element_dense_rank", (F.col("element") + F.col("element_dense_rank")))
)

cleaned = (
    base
    .filter(F.col("element_dense_rank") <= 8)
    .select(
        "*",
        F.when(F.col("element_dense_rank") >= 9, "High").when(F.col("element_dense_rank") >= 4, "Medium").otherwise("Low").alias("element_dense_rank_band"),
        F.greatest("element", "element_plus_element_dense_rank").alias("greatest_element_element_plus_element_dense_rank"),
    )
)

result = (
    cleaned
    .select(
        "effective_from",
        "element",
        "element_dense_rank",
        "element_plus_element_dense_rank",
        "element_dense_rank_band",
        "greatest_element_element_plus_element_dense_rank",
    )
    .orderBy(F.col("element_plus_element_dense_rank").desc_nulls_last())
)
