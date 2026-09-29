from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "*",
        F.ntile(5).over(w).alias("units_bucket"),
        F.ntile(5).over(w2).alias("units_bucket_2"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
