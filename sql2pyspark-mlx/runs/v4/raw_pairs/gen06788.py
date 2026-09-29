from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "category",
        F.first("category").over(w).alias("first_category"),
    )
    .orderBy(F.col("supplier_id").desc(), "id")
)
