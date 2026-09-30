from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_inventory
    .select(
        "*",
        F.ntile(4).over(w).alias("quantity_bucket"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
    )
)
