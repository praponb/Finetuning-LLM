from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("last_restock").between("2024-03-02", "2024-04-18"))
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.round(F.avg("quantity").over(w), 2).alias("quantity_moving_avg_3"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
