from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

cleaned = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6]))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM"))
)

result = (
    cleaned
    .filter(F.col("product_id").isin([5, 7, 9, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_label",
        F.count("quantity").over(w_ordered).alias("last7_count_quantity"),
        F.min("quantity").over(w_ordered).alias("last7_min_quantity"),
        F.avg("quantity").over(w_ordered).alias("last7_avg_quantity"),
        F.sum("quantity").over(w_ordered).alias("last7_sum_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
    )
)
