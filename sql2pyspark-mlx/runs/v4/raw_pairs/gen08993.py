from pyspark.sql import functions as F

quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2024-03-07").cast("date"))
    .select(
        "*",
        F.date_format("last_restock", "MMM yyyy").alias("last_restock_label"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

kept_keys = (
    valid
    .select("store_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity").between(106, 156))
        .select("store_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("last_restock_month_end") < "2024-04-13")
    .select("*", F.date_trunc("week", "last_restock").alias("last_restock_period"))
)

result = (
    filtered
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_label",
        "last_restock_month_end",
        "last_restock_period",
        F.col("min_inventory_quantity"),
    )
)
