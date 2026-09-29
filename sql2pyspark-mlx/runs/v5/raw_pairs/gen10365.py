from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_rounded").asc_nulls_first())
w2 = Window.orderBy(F.col("tier_value_rounded").asc_nulls_last())
w_grp = Window.orderBy(F.col("tier_value").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("tier_value").between(54, 62))
    .select("*", F.round("tier_value", 1).alias("tier_value_rounded"))
)

scored = (
    filtered
    .withColumn("tier_value_rounded_cume_dist", F.cume_dist().over(w))
    .withColumn("tier_value_rounded_rank", F.rank().over(w2))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w_grp))
)

kept_keys = (
    scored
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "enterprise")
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "effective_from",
        "tier_value_rounded",
        "tier_value_rounded_cume_dist",
        "tier_value_rounded_rank",
        "tier_value_dense_rank",
    )
    .orderBy(F.col("tier_value_rounded").desc())
)
