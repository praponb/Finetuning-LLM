from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "*",
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.ntile(4).over(w2).alias("revenue_bucket"),
        F.ntile(3).over(w3).alias("revenue_bucket_2"),
    )
)
