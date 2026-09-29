from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("val_filled")
w3 = Window.orderBy(F.col("val").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

filtered = (
    lines
    .filter(F.col("effective_from") >= F.lit("2024-03-25").cast("date"))
    .select(
        "*",
        F.lower("plan").alias("plan_lower"),
        F.coalesce("val", F.lit(0)).alias("val_filled"),
    )
)

kept_keys = (
    filtered
    .select("id", "plan")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") == "THB")
        .select("id", "plan")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "id",
        "val",
        "val_filled",
        F.max("val_filled").over(w).alias("plan_max_val_filled"),
        F.percent_rank().over(w2).alias("val_filled_percent_rank"),
        F.cume_dist().over(w3).alias("val_cume_dist"),
    )
)

valid = (
    enriched
    .filter(F.col("val_cume_dist") <= 0.47)
    .select(
        "id",
        "val_filled",
        "plan_max_val_filled",
        "val_filled_percent_rank",
        F.when(F.col("plan_max_val_filled") >= 73, "High").when(F.col("plan_max_val_filled") >= 22, "Medium").otherwise("Low").alias("plan_max_val_filled_band"),
        F.floor("val").alias("val_whole"),
    )
)

result = (
    valid
    .select("id", "plan_max_val_filled", "val_filled_percent_rank", "val_whole")
    .orderBy(F.col("val_whole").desc_nulls_last(), "val_filled_percent_rank")
)
