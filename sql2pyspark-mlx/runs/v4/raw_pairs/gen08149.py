from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")

result = (
    df_order_items
    .select(
        "id",
        "product_id",
        "order_id",
        "unit_price",
        F.count("*").over(w).alias("product_id_count"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
    .orderBy(F.col("id").desc())
)
