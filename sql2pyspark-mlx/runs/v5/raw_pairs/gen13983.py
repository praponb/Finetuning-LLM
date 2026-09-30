from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("i.quantity").between(98, 198) | (F.col("s.opened_date") < "2025-07-26"))
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("i.last_restock"),
        F.date_trunc("month", F.col("i.last_restock")).alias("last_restock_period"),
    )
)
