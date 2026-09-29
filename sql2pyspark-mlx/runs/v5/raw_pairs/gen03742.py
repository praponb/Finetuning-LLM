from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w_grp = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("tier_value"))
)

scored = (
    lines
    .select(
        "*",
        F.count("tier_value").over(w_ordered).alias("currency_count_tier_value"),
        F.min("tier_value").over(w_ordered).alias("currency_min_tier_value"),
        F.dense_rank().over(w_grp).alias("tier_value_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("tier_value") != 30)
    .withColumn("currency_length", F.length("currency"))
)

base = (
    valid
    .filter(F.col("currency_min_tier_value") == 73)
    .select("*", F.length("currency").alias("currency_length_2"))
)

result = (
    base
    .select(
        "id",
        "tier_value",
        "currency_count_tier_value",
        "currency_min_tier_value",
        "tier_value_dense_rank",
        "currency_length",
        "currency_length_2",
    )
    .orderBy(F.col("currency_min_tier_value").asc_nulls_last())
)
