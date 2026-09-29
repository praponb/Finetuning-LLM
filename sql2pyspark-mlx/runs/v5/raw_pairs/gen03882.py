from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
)
