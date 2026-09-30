from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"))
)

filtered = (
    enriched
    .filter(F.col("last_restock") >= F.lit("2023-05-31").cast("date"))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
    .withColumn("is_west", F.when(F.col("region") == "West", 1).otherwise(0))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    filtered
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_str",
        "region",
        "last_restock_month_end",
        "is_west",
        "resolved_region",
    )
    .orderBy(F.col("quantity").desc_nulls_last())
)
