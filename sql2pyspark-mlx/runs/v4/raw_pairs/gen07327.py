from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("price", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        "brand",
        "price",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.row_number().over(w2).alias("row_num"),
    )
)
