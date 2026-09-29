from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "outer")
    .filter(F.col("i.product_id").isin([1, 2, 9]))
    .select(
        F.col("i.id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("s.region"),
        F.datediff(F.current_date(), F.col("s.opened_date")).alias("days_since_opened_date"),
    )
    .orderBy("last_restock", "quantity")
)
