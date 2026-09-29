from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "launch_date",
        "stock_qty",
        F.count("*").over(w).alias("category_count"),
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
    )
)
