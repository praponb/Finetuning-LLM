from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-01")))
    .withColumn("last_restock_plus_24d", F.date_add("last_restock", 24))
)

valid = (
    cleaned
    .filter((F.col("last_restock") > F.lit("2023-10-14").cast("date")) & (F.year("last_restock") >= 2023))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock_plus_24d",
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
)

remaining_keys = (
    valid
    .select("store_id", "product_id")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(79, 108))
        .select("store_id", "product_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.brand"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("week", "last_restock_plus_24d").alias("week_start"))
    .agg(
        F.percentile_approx("adjusted_quantity", 0.9).alias("adjusted_quantity_p90"),
        F.round(F.sum("adjusted_quantity") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("adjusted_quantity_per_stark"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("adjusted_quantity")).otherwise(0)).alias("umbrella_adjusted_quantity"),
        F.round(F.sum("adjusted_quantity") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("adjusted_quantity_per_stark_2"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
    )
    .filter(F.col("adjusted_quantity_per_stark_2") > 0)
)
