from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id")
w_hist = Window.orderBy(F.col("quantity").desc_nulls_last())
w_recent = Window.partitionBy("store_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "id",
        "product_id",
        "quantity",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.avg("quantity").over(w2).alias("product_id_avg_quantity"),
        F.rank().over(w_hist).alias("quantity_rank"),
        F.lag("quantity", 2).over(w_recent).alias("prev_quantity"),
    )
)
