from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.dense_rank().over(w).alias("last_restock_dense_rank"),
        F.row_number().over(w2).alias("row_num"),
    )
)
