from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("last_restock").between("2024-09-01", "2024-09-18") & F.month("last_restock").isin([3, 5, 8]) & (F.col("product_id") == 4))
)

agg = (
    filtered_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
    )
)

result = (
    agg
    .filter(F.col("quantity_range") > 61)
    .select("product_id", "quantity_range", "last_last_restock")
    .orderBy(F.col("product_id").desc())
)
