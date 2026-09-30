from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("price") > 980)
    .select(
        "id",
        "category",
        "supplier_id",
        "launch_date",
        "cost",
        "brand",
        "stock_qty",
        F.sum("cost").over(w).alias("running_cost"),
        F.lead("stock_qty", 1, 0).over(w2).alias("next_stock_qty"),
    )
)
