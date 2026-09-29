from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .filter(F.col("cost") > 580)
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "price",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") == 1)
    .select("id", "supplier_id", "stock_qty", "price")
)
