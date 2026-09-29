from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id")

result = (
    df_order_items
    .filter(F.col("product_id") == 12)
    .select(
        "id",
        "product_id",
        "quantity",
        F.ntile(4).over(w).alias("bucket"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 3).alias("quantity_share"),
    )
)
