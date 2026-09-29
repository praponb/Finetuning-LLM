from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.date_sub(F.col("s.opened_date"), 29).alias("opened_date_minus_29d"),
    )
    .orderBy("opened_date_minus_29d", F.col("quantity").desc())
)
