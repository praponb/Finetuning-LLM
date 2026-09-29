from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w_hist = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .select(
        "*",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.ntile(4).over(w_hist).alias("cost_bucket_2"),
    )
)
