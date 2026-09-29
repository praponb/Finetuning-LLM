from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") != 158) & (F.col("quantity") <= 202))
    .select("id", "store_id", "quantity")
    .orderBy("id")
)
