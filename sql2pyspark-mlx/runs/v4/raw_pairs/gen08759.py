from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("element")
w2 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist_2").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w6 = Window.orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

enriched = (
    flattened
    .select(
        "*",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.cume_dist().over(w2).alias("element_cume_dist_2"),
        F.cume_dist().over(w3).alias("element_cume_dist_3"),
        F.rank().over(w).alias("element_rank"),
        F.cume_dist().over(w).alias("element_cume_dist_4"),
    )
)

base = (
    enriched
    .filter(F.col("element") > 68)
    .select(
        "id",
        "currency",
        "element",
        "element_cume_dist_2",
        F.when(F.col("element_cume_dist_3") >= 0.66, "High").when(F.col("element_cume_dist_3") >= 0.29, "Medium").otherwise("Low").alias("element_cume_dist_3_band"),
    )
)

scored = (
    base
    .withColumn("element_cume_dist_2_rank", F.rank().over(w4))
    .withColumn("element_rank", F.rank().over(w5))
    .withColumn("element_dense_rank", F.dense_rank().over(w6))
    .withColumn("element_rank_2", F.rank().over(w3))
    .withColumn("element_dense_rank_2", F.dense_rank().over(w))
)

result = (
    scored
    .select(
        "id",
        "currency",
        "element",
        "element_cume_dist_2",
        "element_cume_dist_3_band",
        "element_rank",
        "element_dense_rank",
        "element_rank_2",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "GROUP_B").otherwise("GROUP_A").alias("currency_zone"),
    )
)
