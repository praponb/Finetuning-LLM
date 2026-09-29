from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("product_name").like("W%"))
    .select(
        "id",
        "brand",
        "stock_qty",
        F.max("stock_qty").over(w).alias("brand_max_stock_qty"),
        F.row_number().over(w2).alias("rn"),
    )
)
