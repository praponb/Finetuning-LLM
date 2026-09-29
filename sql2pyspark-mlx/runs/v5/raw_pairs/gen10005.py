from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

latest = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(quantity_by_store_id, on="store_id", how="left")
    .select("id", "store_id", "units", "revenue", "rn", F.col("min_inventory_quantity"))
)
