from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]))
    .select(
        "id",
        "product_id",
        "sale_date",
        F.dense_rank().over(w).alias("sale_date_dense_rank"),
    )
    .orderBy(F.col("sale_date_dense_rank").desc())
)
