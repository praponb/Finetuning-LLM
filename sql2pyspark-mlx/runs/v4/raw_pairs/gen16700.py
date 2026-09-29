from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "category",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("id").desc())
)
