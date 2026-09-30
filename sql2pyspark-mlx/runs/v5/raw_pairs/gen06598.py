from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")
w2 = Window.orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("category") != "Electronics")
    .select(
        "id",
        "category",
        "launch_date",
        "cost",
        "stock_qty",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
        F.lead("stock_qty", 1).over(w2).alias("next_stock_qty"),
    )
)
