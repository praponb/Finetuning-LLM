from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("val"))
)

scored = (
    exploded
    .select(
        "*",
        F.max("val").over(w).alias("plan_max_val"),
        F.dense_rank().over(w2).alias("val_dense_rank"),
    )
)

base = (
    scored
    .filter(F.month("effective_from").isin([3, 4, 5]))
    .withColumn("plan_max_val_rounded", F.round("plan_max_val", 1))
)

remaining_keys = (
    base
    .select("plan", "id")
    .subtract(
        df_price_tiers
        .filter(F.col("currency") != "USD")
        .select("plan", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("plan_max_val_rounded").isNotNull())
    .select(
        "*",
        F.coalesce("val", F.lit(0)).alias("val_filled"),
        (F.col("val_dense_rank") - F.col("plan_max_val")).alias("val_dense_rank_minus_plan_max_val"),
    )
)

base_price_tiers = (
    cleaned
    .filter((F.col("effective_from") >= F.lit("2024-01-31").cast("date")) & F.col("plan_max_val_rounded").between(23, 77))
    .withColumn("plan_max_val_rounded_times_plan_max_val", (F.col("plan_max_val_rounded") * F.col("plan_max_val")))
    .withColumn("clean_plan", F.trim("plan"))
)

result = (
    base_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "val_dense_rank",
        "plan_max_val_rounded",
        "val_filled",
        "val_dense_rank_minus_plan_max_val",
        "plan_max_val_rounded_times_plan_max_val",
        F.when(F.col("plan").rlike("^(enterprise|starter)$"), "GROUP_B").when(F.col("plan").rlike("^(basic|pro)$"), "GROUP_A").otherwise("PRIORITY").alias("plan_zone"),
    )
)
