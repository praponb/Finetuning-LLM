from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 9, 10]))
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        "last_restock",
        F.rank().over(w).alias("quantity_rank"),
        F.round(F.avg("quantity").over(w2), 2).alias("quantity_moving_avg_3"),
    )
)
