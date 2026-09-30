from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id") == 2)
    .select(
        "id",
        "store_id",
        "quantity",
        F.avg("quantity").over(w).alias("quantity_avg_6_prior"),
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
        F.lead("quantity", 1).over(w4).alias("next_quantity_2"),
    )
)
