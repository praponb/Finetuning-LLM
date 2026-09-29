from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

ranked = (
    df_inventory
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock")
)

result = (
    ranked
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "product_id", "quantity", F.col("avg_inventory_quantity"))
)
