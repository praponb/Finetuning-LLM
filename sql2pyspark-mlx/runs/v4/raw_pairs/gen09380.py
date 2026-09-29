from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "id",
        "product_id",
        "revenue",
        F.min("revenue").over(w).alias("product_id_min_revenue"),
    )
)
