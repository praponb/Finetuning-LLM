from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "product_name",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "supplier_id", "stock_qty", "product_name")
)
