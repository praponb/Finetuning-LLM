from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        F.rank().over(w).alias("rnk"),
        F.ntile(2).over(w2).alias("bucket"),
    )
)
