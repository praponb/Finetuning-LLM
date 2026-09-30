from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-11-30")
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("last_restock_month"))
    .agg(
        F.count("*").alias("inventory_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 57)
    .select("last_restock_month", "inventory_count", "quantity_range")
)
