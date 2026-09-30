from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")
w_ordered = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.last("sqft", ignorenulls=True).over(w_ordered).alias("last_known_sqft"),
        F.ntile(3).over(w_running).alias("sqft_bucket_2"),
    )
)
