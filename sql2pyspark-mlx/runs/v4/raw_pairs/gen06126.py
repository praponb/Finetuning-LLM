from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_recent = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("quantity").desc_nulls_first())
w4 = Window.orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "*",
        F.lead("quantity", 2).over(w_part).alias("next_quantity"),
        F.sum("quantity").over(w_recent).alias("cumulative_quantity"),
        F.sum("quantity").over(w_recent).alias("running_quantity"),
        F.dense_rank().over(w3).alias("quantity_dense_rank"),
        F.ntile(3).over(w4).alias("quantity_bucket"),
    )
)
