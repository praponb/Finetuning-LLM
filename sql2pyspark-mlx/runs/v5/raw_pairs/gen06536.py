from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("stock_qty") == 365)
    .select(
        "id",
        "category",
        "stock_qty",
        "supplier_id",
        "launch_date",
        F.rank().over(w).alias("rnk"),
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
    )
)
