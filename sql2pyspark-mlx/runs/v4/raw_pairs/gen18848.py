from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        F.min("stock_qty").over(w).alias("supplier_id_min_stock_qty"),
    )
)
