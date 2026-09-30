from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") < "2024-10-24")
    .orderBy(F.col("last_restock").desc())
)
