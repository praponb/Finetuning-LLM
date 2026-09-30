from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_inventory
    .withColumn("product_id_count_quantity", F.count("quantity").over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "product_id_count_quantity",
        "running_quantity",
    )
    .orderBy("running_quantity", F.col("id").desc())
)
