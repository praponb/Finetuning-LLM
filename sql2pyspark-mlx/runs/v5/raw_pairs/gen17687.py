from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

enriched = (
    exploded
    .select(
        "*",
        F.avg("val").over(w).alias("currency_avg_val"),
        F.cume_dist().over(w2).alias("val_cume_dist"),
        F.count("val").over(w3).alias("plan_count_val"),
    )
)

cleaned = (
    enriched
    .filter(F.col("effective_from") > F.lit("2024-05-01").cast("date"))
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("val_cume_dist").between(0.35, 0.78))
    .select(
        "id",
        "plan",
        "currency",
        "val_cume_dist",
        "plan_count_val",
        "plan_prefix",
        F.round((F.col("val_cume_dist") * 1.07), 2).alias("val_cume_dist_scaled"),
    )
)

kept_keys = (
    cleaned_price_tiers
    .select("id", "plan")
    .intersect(
        df_price_tiers
        .filter(F.month("effective_from").isin([2, 8, 11]))
        .select("id", "plan")
    )
)

verified = (
    cleaned_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("plan", "currency", "val_cume_dist", "plan_prefix", "val_cume_dist_scaled")
    .orderBy("val_cume_dist_scaled")
)
