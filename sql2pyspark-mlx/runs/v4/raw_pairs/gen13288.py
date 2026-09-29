from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.ntile(10).over(w).alias("revenue_bucket"),
        F.ntile(3).over(w2).alias("units_bucket"),
    )
)
