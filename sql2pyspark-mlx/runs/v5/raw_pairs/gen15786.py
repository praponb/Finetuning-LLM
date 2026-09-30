from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category")

result = (
    df_products
    .select(
        "id",
        "brand",
        "category",
        "stock_qty",
        F.max("stock_qty").over(w).alias("brand_max_stock_qty"),
    )
)
