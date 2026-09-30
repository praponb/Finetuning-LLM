from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity",
        "store_id",
        (F.col("quantity") - F.lag("quantity", 1, 0).over(w)).alias("quantity_change"),
        F.percent_rank().over(w2).alias("quantity_pct_rank"),
    )
)
