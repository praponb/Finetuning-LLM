from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("revenue")

result = (
    df_store_sales
    .filter(F.col("units") >= 15)
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.count("*").over(w).alias("store_id_count"),
        F.rank().over(w2).alias("revenue_rank"),
    )
    .orderBy(F.col("revenue_rank").desc(), F.col("product_id").desc())
)
