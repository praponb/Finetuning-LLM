from pyspark.sql import functions as F

units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

kept_keys = (
    df_inventory
    .select("id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2023-03-23")
        .select("id", "store_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
