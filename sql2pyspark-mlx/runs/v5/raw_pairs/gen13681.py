from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")

result = (
    df_inventory
    .filter(F.month("last_restock").isin([1, 6, 10]))
    .select(
        "id",
        "product_id",
        "last_restock",
        F.rank().over(w).alias("rnk"),
        F.ntile(3).over(w2).alias("last_restock_ntile"),
    )
    .orderBy("last_restock_ntile")
)
