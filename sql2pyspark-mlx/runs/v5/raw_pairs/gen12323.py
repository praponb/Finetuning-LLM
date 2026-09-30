from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        "launch_date",
        F.percent_rank().over(w).alias("stock_qty_pct_rank"),
        (F.col("stock_qty") - F.lag("stock_qty", 2).over(w2)).alias("stock_qty_change"),
    )
)
