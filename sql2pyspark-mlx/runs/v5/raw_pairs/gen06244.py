from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element_filled").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("element_filled").desc_nulls_first())
w3 = Window.partitionBy("currency")
w4 = Window.orderBy("element_filled_cume_dist_2")
w5 = Window.partitionBy("currency").orderBy(F.col("element_filled_cume_dist_2").desc())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_prices").alias("element"))
)

base = (
    lines
    .filter(F.col("element") > 47)
    .select("*", F.substring("currency", 1, 3).alias("currency_prefix"))
)

cleaned = (
    base
    .filter(F.col("currency") == "THB")
    .select("*", F.coalesce("element", F.lit(0)).alias("element_filled"))
)

metrics = (
    cleaned
    .filter(~F.col("currency").isin(["EUR", "THB"]))
    .select(
        "id",
        "currency",
        F.cume_dist().over(w).alias("element_filled_cume_dist"),
        F.cume_dist().over(w2).alias("element_filled_cume_dist_2"),
        F.min("element_filled").over(w3).alias("currency_min_element_filled"),
    )
)

result = (
    metrics
    .withColumn("element_filled_cume_dist_2_percent_rank", F.percent_rank().over(w4))
    .withColumn("element_filled_cume_dist_2_dense_rank", F.dense_rank().over(w5))
)
