from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "price",
        F.dense_rank().over(w).alias("price_dense_rank"),
    )
)
