from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_last())
w3 = Window.orderBy(F.col("tier_value").asc_nulls_first())
w_hist = Window.partitionBy("currency").orderBy(F.col("least_tier_value_cume_dist_int_tier_value").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_sum_tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("tier_limits").alias("tier_value"))
)

with_windows = (
    exploded
    .filter(F.col("currency").isin(["THB", "USD"]))
    .withColumn("currency_min_tier_value", F.min("tier_value").over(w))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
    .withColumn("tier_value_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("currency_sum_tier_value", F.sum("tier_value").over(w))
    .withColumn("currency_sum_tier_value_2", F.sum("tier_value").over(w))
)

filtered = (
    with_windows
    .filter(F.col("currency_sum_tier_value").between(115, 396))
    .select(
        "*",
        F.col("tier_value_cume_dist").cast("int").alias("tier_value_cume_dist_int"),
    )
)

filtered_price_tiers = (
    filtered
    .filter(F.col("tier_value_cume_dist").between(0.25, 0.54))
    .select(
        "id",
        "currency",
        "tier_value",
        "currency_min_tier_value",
        "tier_value_cume_dist",
        "tier_value_cume_dist_2",
        "currency_sum_tier_value",
        F.least("tier_value_cume_dist_int", "tier_value").alias("least_tier_value_cume_dist_int_tier_value"),
    )
)

scored = (
    filtered_price_tiers
    .select(
        "id",
        "tier_value_cume_dist_2",
        "currency_sum_tier_value",
        F.cume_dist().over(w_hist).alias("least_tier_value_cume_dist_int_tier_value_cume_dist"),
        F.min("currency_min_tier_value").over(w).alias("currency_min_currency_min_tier_value"),
        F.dense_rank().over(w5).alias("currency_sum_tier_value_dense_rank"),
    )
)

result = (
    scored
    .cube(
        F.when(F.col("least_tier_value_cume_dist_int_tier_value_cume_dist") >= 0.81, "HIGH").otherwise("LOW").alias("least_tier_value_cume_dist_int_tier_value_cume_dist_tier"),
        F.when(F.col("currency_sum_tier_value") >= 401, "PREMIUM").otherwise("STANDARD").alias("currency_sum_tier_value_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum("currency_min_currency_min_tier_value").alias("total_currency_min_currency_min_tier_value"),
        F.percentile_approx("currency_min_currency_min_tier_value", [0.25, 0.5, 0.75]).alias("currency_min_currency_min_tier_value_quartiles"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("total_currency_min_currency_min_tier_value").desc())
)
