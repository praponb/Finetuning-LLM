from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last())
w2 = Window.orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "*",
        F.cume_dist().over(w).alias("revenue_cume_dist"),
        F.ntile(3).over(w2).alias("revenue_bucket"),
    )
)
