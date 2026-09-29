from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand")
w2 = Window.partitionBy("supplier_id")
w3 = Window.orderBy("price", "id")

result = (
    df_products
    .select(
        "*",
        F.sum("price").over(w).alias("supplier_id_sum_price"),
        F.corr("price", "cost").over(w2).alias("corr_price_cost"),
        F.lag("price", 1).over(w3).alias("prev_price"),
    )
)
