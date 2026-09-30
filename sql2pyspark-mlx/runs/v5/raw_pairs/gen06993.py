from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock").between("2023-09-06", "2024-03-01") & (F.col("quantity") != 163))
    .select("last_restock", "quantity")
    .orderBy(F.col("last_restock").desc())
)
