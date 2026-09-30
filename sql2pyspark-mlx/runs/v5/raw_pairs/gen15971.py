from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        F.dense_rank().over(w).alias("last_restock_dense_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
