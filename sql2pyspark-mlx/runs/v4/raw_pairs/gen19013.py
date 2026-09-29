from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "product_id", "quantity", "last_restock")
)
