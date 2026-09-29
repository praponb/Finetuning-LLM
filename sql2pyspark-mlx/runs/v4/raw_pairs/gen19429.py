from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())

result = (
    df_inventory
    .select(
        "*",
        F.avg("quantity").over(w).alias("quantity_avg_2_prior"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
        F.rank().over(w3).alias("quantity_rank"),
    )
)
