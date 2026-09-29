from pyspark.sql import functions as F

min_quantity = df_inventory.filter(F.col("quantity") != 87).agg(F.min("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") <= min_quantity)
    .select("id", "quantity", "last_restock", "store_id")
    .orderBy(F.col("last_restock").desc())
)
