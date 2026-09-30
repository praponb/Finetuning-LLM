from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 77, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("row_count") < 1)
)
