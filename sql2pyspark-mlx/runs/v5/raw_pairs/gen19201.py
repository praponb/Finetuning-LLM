from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .filter(F.col("supplier_id") == 20)
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        F.row_number().over(w).alias("stock_qty_row"),
    )
)
