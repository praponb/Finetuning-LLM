from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w_ordered = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("plan_length").desc_nulls_first())
w4 = Window.partitionBy("currency")
w_grp = Window.partitionBy("currency").orderBy("plan_length")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

base = (
    exploded
    .filter(F.col("plan") == "basic")
    .withColumn("plan_length", F.length("plan"))
)

remaining_keys = (
    base
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") < "2025-01-30")
        .select("id", "plan")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "currency",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.cume_dist().over(w_ordered).alias("val_cume_dist"),
        F.percent_rank().over(w3).alias("plan_length_percent_rank"),
        F.min("plan_length").over(w4).alias("currency_min_plan_length"),
        F.percent_rank().over(w_grp).alias("plan_length_percent_rank_2"),
    )
)

base_price_tiers = (
    enriched
    .filter(F.col("currency") == "USD")
    .select(
        "currency",
        "val_cume_dist",
        "currency_min_plan_length",
        "plan_length_percent_rank_2",
        F.when(F.col("currency").isin(["THB", "USD"]), "Tier 1").otherwise("Other").alias("currency_group"),
    )
)

filtered = (
    base_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB", "USD"]))
    .select("*", F.lower("currency").alias("currency_lower"))
)

result = (
    filtered
    .select("currency", "plan_length_percent_rank_2", "val_cume_dist")
    .unpivot(
        ids=["currency"],
        values=["plan_length_percent_rank_2", "val_cume_dist"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
)
