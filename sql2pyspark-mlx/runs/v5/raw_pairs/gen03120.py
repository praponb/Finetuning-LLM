from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
