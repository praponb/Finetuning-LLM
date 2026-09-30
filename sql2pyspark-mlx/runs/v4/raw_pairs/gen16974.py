from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan_length_tier").orderBy(F.col("num_plans").desc())

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("val"))

valid = (
    lines
    .filter(F.col("plan") != "basic")
    .select("*", F.round((F.col("val") * 1.1), 2).alias("adjusted_val"))
)

filtered = (
    valid
    .filter(F.col("val").between(49, 80))
    .withColumn("greatest_adjusted_val_val", F.greatest("adjusted_val", "val"))
)

valid_price_tiers = (
    filtered
    .filter(F.col("val").isNotNull())
    .select(
        "id",
        "plan",
        "val",
        F.col("adjusted_val").cast("int").alias("adjusted_val_int"),
    )
)

filtered_price_tiers = (
    valid_price_tiers
    .filter(F.col("adjusted_val_int").isNotNull())
    .withColumn("greatest_val_adjusted_val_int", F.greatest("val", "adjusted_val_int"))
    .withColumn("plan_length", F.length("plan"))
)

kept_keys = (
    filtered_price_tiers
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.month("effective_from").isin([3, 9, 10]))
        .select("id")
    )
)

kept_rows = (
    filtered_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    kept_rows
    .groupBy(
        F.when(F.col("plan_length") >= 69, "LARGE").otherwise("SMALL").alias("plan_length_tier"),
        "plan",
    )
    .agg(
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_rows"),
        F.sum(F.when(F.col("plan") == "starter", F.col("val"))).alias("starter_total_val"),
        F.sum("adjusted_val_int").alias("total_adjusted_val_int"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .withColumn("num_plans_percent_rank", F.percent_rank().over(w))
)

result = (
    totals
    .select(
        "plan_length_tier",
        "plan",
        "starter_total_val",
        "total_adjusted_val_int",
        "plan_list",
        "num_plans",
        "num_plans_percent_rank",
    )
    .orderBy(F.col("plan_length_tier").asc_nulls_last())
)
