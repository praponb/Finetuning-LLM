from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4]))
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
    .orderBy(F.col("dense_rnk").desc(), F.col("id").desc())
)
