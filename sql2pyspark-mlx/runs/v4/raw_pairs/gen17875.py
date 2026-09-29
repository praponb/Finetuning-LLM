from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") <= 3800)
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "product_id",
        F.lag("revenue", 1, 0).over(w).alias("prev_revenue"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue_2"),
    )
)
