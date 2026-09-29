from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_plus_45d", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock_plus_45d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.floor("quantity").alias("quantity_whole"),
        F.date_add("last_restock", 45).alias("last_restock_plus_45d"),
    )
)

result = (
    base
    .withColumn("quantity_whole_avg_5", F.avg("quantity_whole").over(w))
    .withColumn("last_known_quantity_whole", F.last("quantity_whole", ignorenulls=True).over(w2))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w3))
)
