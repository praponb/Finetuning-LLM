from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w_hist = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("category", "brand").orderBy(F.col("launch_date").desc(), "id")

result = (
    df_products
    .filter(F.col("supplier_id").isin([3, 6, 23]))
    .select(
        "*",
        F.lag("stock_qty", 1).over(w).alias("prev_stock_qty"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.row_number().over(w3).alias("event_seq_2"),
    )
)
