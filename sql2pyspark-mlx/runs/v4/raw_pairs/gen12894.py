from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("tier_value_filled").asc_nulls_first())
w4 = Window.orderBy(F.col("tier_value").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("tier_value"))
)

filtered = (
    lines
    .filter(F.col("effective_from") >= F.lit("2024-09-01").cast("date"))
    .select("*", F.coalesce("tier_value", F.lit(0)).alias("tier_value_filled"))
)

with_windows = (
    filtered
    .withColumn("plan_max_tier_value_filled", F.max("tier_value_filled").over(w_hist))
    .withColumn("tier_value_rank", F.rank().over(w2))
    .withColumn("tier_value_filled_rank", F.rank().over(w3))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w4))
    .withColumn("plan_min_tier_value", F.min("tier_value").over(w_hist))
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "tier_value_filled",
        "tier_value_rank",
        "tier_value_filled_rank",
        "tier_value_dense_rank",
        "plan_min_tier_value",
        F.when(F.col("plan").rlike("^(pro|starter)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_A").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("tier_value_filled_rank").desc_nulls_last())
)
