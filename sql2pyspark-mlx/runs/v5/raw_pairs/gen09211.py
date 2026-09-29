from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "category").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .filter(F.col("supplier_id").isin([4, 24]))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "launch_date",
        F.row_number().over(w).alias("row_seq"),
        F.avg("stock_qty").over(w2).alias("stock_qty_avg_6"),
    )
)
