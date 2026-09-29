from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("quantity") == 176)
    .select(
        "id",
        "product_id",
        "quantity",
        "store_id",
        F.rank().over(w).alias("rnk"),
        F.row_number().over(w2).alias("quantity_row"),
    )
    .orderBy(F.col("product_id").desc())
)
