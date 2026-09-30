from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("element").desc_nulls_last())
w_grp_2 = Window.partitionBy("currency", "plan").orderBy(F.col("element").asc_nulls_first())
w_grp_3 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .withColumn("element_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("element_rank", F.rank().over(w_grp_2))
    .withColumn("element_cume_dist", F.cume_dist().over(w_grp_3))
)
