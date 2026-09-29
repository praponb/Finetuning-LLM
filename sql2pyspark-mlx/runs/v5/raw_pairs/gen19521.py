from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand")

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Fashion", "Sports"]))
    .select(
        "id",
        "category",
        "launch_date",
        "stock_qty",
        "brand",
        "cost",
        F.sum("stock_qty").over(w).alias("running_stock_qty"),
        F.min("cost").over(w2).alias("brand_min_cost"),
    )
)
