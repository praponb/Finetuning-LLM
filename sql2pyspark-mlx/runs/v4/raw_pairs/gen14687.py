from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "quantity",
        "order_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") == 1)
    .select("id", "product_id", "quantity", "order_id")
    .orderBy(F.col("id").desc())
)
