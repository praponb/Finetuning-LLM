from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")

result = (
    df_products
    .select(
        "id",
        "brand",
        "stock_qty",
        F.max("stock_qty").over(w).alias("brand_max_stock_qty"),
    )
    .orderBy("brand")
)
