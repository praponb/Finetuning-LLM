from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

cleaned = (
    exploded
    .filter(F.col("effective_from") >= F.lit("2024-02-16").cast("date"))
    .withColumn("is_starter", F.when(F.col("plan") == "starter", 1).otherwise(0))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("plan") == "starter")
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

filtered = (
    cleaned_price_tiers
    .filter(F.month("effective_from").isin([4, 8, 12]))
    .select(
        "id",
        "effective_from",
        "val",
        "is_starter",
        F.coalesce("is_starter", F.lit(0)).alias("is_starter_filled"),
        F.when(F.col("plan").isin(["basic", "starter"]), "Priority").otherwise("Other").alias("plan_group"),
    )
)

result = (
    filtered
    .select("id", "effective_from", "val", "plan_group")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
