from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.percent_rank().over(w).alias("quantity_percent_rank"),
        F.avg("quantity").over(w2).alias("quantity_avg_3"),
    )
)
