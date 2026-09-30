from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc())
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .filter(F.col("quantity") != 4)
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
        F.lead("quantity", 1).over(w2).alias("next_quantity"),
    )
)
