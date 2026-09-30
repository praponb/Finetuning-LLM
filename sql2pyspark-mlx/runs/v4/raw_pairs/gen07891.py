from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 5, 7]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "product_id", "last_restock", "quantity")
    .orderBy("quantity", "last_restock")
)
