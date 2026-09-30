from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "quantity", "last_restock")
)
