from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("element"))

result = (
    flattened
    .select(
        "id",
        "plan",
        "element",
        F.when(F.col("plan").rlike("^(starter|pro)$"), "GROUP_B").when(F.col("plan").rlike("^(basic|enterprise)$"), "PRIORITY").otherwise("CORE").alias("plan_zone"),
    )
)
