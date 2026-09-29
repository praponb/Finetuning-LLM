from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("element"))

kept_keys = (
    lines
    .select("id")
    .intersectAll(
        df_price_tiers
        .filter(F.year("effective_from") >= 2023)
        .select("id")
    )
)

reconciled = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    reconciled
    .filter(F.col("plan").isin(["enterprise", "basic", "pro"]))
    .select("*", F.round((F.col("element") * 0.9), 2).alias("adjusted_element"))
)

result = (
    base
    .select(
        "id",
        "plan",
        F.when(F.col("plan").rlike("^(starter|pro)$"), "GROUP_B").otherwise("PRIORITY").alias("plan_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
