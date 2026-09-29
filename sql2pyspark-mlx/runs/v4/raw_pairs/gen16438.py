from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.sqft") > 7498) & (F.col("i.last_restock") > "2025-01-29"))
    .select(
        F.col("s.sqft"),
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.last_day(F.col("i.last_restock")).alias("last_restock_month_end"),
    )
    .orderBy("product_id")
)
