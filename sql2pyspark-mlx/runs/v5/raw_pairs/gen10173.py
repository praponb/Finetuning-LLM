from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_4"),
        F.rank().over(w2).alias("last_restock_rank"),
    )
)
