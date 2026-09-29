from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "store_id",
        "quantity",
        F.max("quantity").over(w).alias("store_id_max_quantity"),
    )
    .orderBy("store_id_max_quantity")
)
