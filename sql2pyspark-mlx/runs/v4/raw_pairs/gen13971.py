from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "quantity",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
    .orderBy(F.col("id").desc(), "last_restock")
)
