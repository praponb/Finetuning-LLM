from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("element")
w_recent = Window.orderBy(F.col("element").desc_nulls_last())
w_hist = Window.orderBy(F.col("element").desc())
w4 = Window.orderBy(F.col("element").asc_nulls_first())

exploded = df_price_tiers.select("id", F.explode_outer("tier_limits").alias("element"))

result = (
    exploded
    .select(
        "id",
        "element",
        F.dense_rank().over(w_grp).alias("element_dense_rank"),
        F.cume_dist().over(w_recent).alias("element_cume_dist"),
        F.cume_dist().over(w_hist).alias("element_cume_dist_2"),
        F.cume_dist().over(w4).alias("element_cume_dist_3"),
        F.cume_dist().over(w_hist).alias("element_cume_dist_4"),
    )
)
