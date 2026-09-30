from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("supplier_id")

result = (
    df_products
    .filter(F.col("supplier_id") == 29)
    .select(
        "id",
        "brand",
        "category",
        "price",
        "supplier_id",
        "stock_qty",
        F.row_number().over(w).alias("rn"),
        F.avg("stock_qty").over(w2).alias("supplier_id_avg_stock_qty"),
    )
    .orderBy("category", F.col("price").desc())
)
