from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "price",
        "supplier_id",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("supplier_id").desc(), F.col("pos").desc())
)
