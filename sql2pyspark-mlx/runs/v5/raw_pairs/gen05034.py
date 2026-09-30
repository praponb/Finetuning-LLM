from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("supplier_id", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last())
w5 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.lag("price", 1, 0).over(w).alias("prev_price"),
        F.sum("price").over(w2).alias("cumulative_price"),
        F.ntile(10).over(w3).alias("stock_qty_bucket"),
        F.dense_rank().over(w4).alias("stock_qty_dense_rank"),
        F.lead("stock_qty", 1).over(w5).alias("next_stock_qty"),
    )
)
