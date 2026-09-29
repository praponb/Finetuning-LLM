from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2024-04-01") & (F.col("quantity") >= 65))
)
