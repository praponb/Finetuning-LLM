from pyspark.sql import functions as F

avg_quantity = df_inventory.filter(F.col("last_restock") <= "2023-09-01").agg(F.avg("quantity")).first()[0]

result = (
    df_inventory
    .filter(F.col("quantity") > avg_quantity)
    .select("id", "quantity")
)
