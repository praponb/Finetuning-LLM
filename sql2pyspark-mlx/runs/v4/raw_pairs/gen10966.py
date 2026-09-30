from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "store_id",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "last_restock", "store_id")
)
