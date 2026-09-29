from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("revenue", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_store_sales
    .select(
        "*",
        F.avg("revenue").over(w).alias("store_id_avg_revenue"),
        F.avg("revenue").over(w2).alias("revenue_avg_5"),
    )
)
