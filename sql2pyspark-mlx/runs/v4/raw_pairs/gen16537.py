from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        F.sum("revenue").over(w).alias("store_id_sum_revenue"),
    )
)
