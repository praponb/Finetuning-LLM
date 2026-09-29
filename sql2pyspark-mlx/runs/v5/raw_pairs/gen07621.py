from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .select(
        "id",
        "revenue",
        "store_id",
        F.first("store_id").over(w).alias("first_store_id"),
    )
)
