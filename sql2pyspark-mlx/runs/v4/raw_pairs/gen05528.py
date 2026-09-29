from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock") < "2025-06-01")
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
)

result = (
    valid
    .withColumn("last_restock_weekofyear_tier", F.when(F.col("last_restock_weekofyear") >= 62, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 194, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["last_restock_weekofyear_tier", "store_id", "quantity_tier"], ["last_restock_weekofyear_tier", "store_id"]],
        "last_restock_weekofyear_tier",
        "store_id",
        "quantity_tier",
    )
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("quantity_p50") >= 0)
    .orderBy(F.col("quantity_p50").desc_nulls_last())
)
