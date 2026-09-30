from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.max("quantity").over(w).alias("last7_max_quantity"),
        F.count("quantity").over(w).alias("last7_count_quantity"),
        F.sum("quantity").over(w).alias("last7_sum_quantity"),
        F.avg("quantity").over(w).alias("last7_avg_quantity"),
    )
)
