from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "brand",
        "launch_date",
        F.row_number().over(w).alias("stock_qty_row"),
        F.rank().over(w2).alias("launch_date_rank"),
    )
)
