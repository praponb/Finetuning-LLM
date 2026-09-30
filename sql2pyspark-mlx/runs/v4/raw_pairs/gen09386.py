from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_enterprise").desc_nulls_last())
w2 = Window.orderBy(F.col("is_enterprise").desc())
w3 = Window.orderBy(F.col("is_enterprise_cume_dist").asc_nulls_first())
w4 = Window.orderBy(F.col("is_enterprise_percent_rank").desc())
w5 = Window.orderBy(F.col("is_enterprise_cume_dist").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

base = (
    exploded
    .filter(F.col("val") != 60)
    .select(
        "id",
        "effective_from",
        F.when(F.col("plan") == "enterprise", 1).otherwise(0).alias("is_enterprise"),
    )
)

with_windows = (
    base
    .withColumn("is_enterprise_cume_dist", F.cume_dist().over(w))
    .withColumn("is_enterprise_percent_rank", F.percent_rank().over(w2))
)

with_windows_price_tiers = (
    with_windows
    .withColumn("is_enterprise_cume_dist_rank", F.rank().over(w3))
    .withColumn("is_enterprise_percent_rank_rank", F.rank().over(w4))
    .withColumn("is_enterprise_cume_dist_cume_dist", F.cume_dist().over(w5))
)

result = (
    with_windows_price_tiers
    .select(
        "id",
        "is_enterprise",
        "is_enterprise_percent_rank",
        "is_enterprise_cume_dist_rank",
        "is_enterprise_cume_dist_cume_dist",
    )
    .orderBy(F.col("is_enterprise_cume_dist_rank").desc())
)
