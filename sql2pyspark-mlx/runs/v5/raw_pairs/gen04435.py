from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "store_id", "last_restock", "product_id")
)
