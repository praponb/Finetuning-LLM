from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_inventory
    .filter(F.month("last_restock").isin([2, 3, 5]))
    .select(
        "*",
        F.lag("quantity", 1).over(w).alias("prev_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_5"),
    )
)

result = (
    scored
    .select("id", "store_id", "product_id", "quantity", "last_restock", "prev_quantity")
)
