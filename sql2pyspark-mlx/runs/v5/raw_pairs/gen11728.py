from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .filter(F.col("quantity") <= 205)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
