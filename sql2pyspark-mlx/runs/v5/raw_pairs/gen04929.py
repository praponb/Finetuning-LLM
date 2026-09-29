from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc())

result = (
    df_products
    .filter(F.length("product_name") < 14)
    .select(
        "id",
        "supplier_id",
        "price",
        "brand",
        F.row_number().over(w).alias("row_num"),
        F.dense_rank().over(w2).alias("price_dense_rank"),
    )
    .orderBy(F.col("id").desc())
)
