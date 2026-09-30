from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 10]))
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        "units",
        F.min("revenue").over(w).alias("store_id_min_revenue"),
        (F.col("units") - F.lag("units", 1).over(w2)).alias("units_change"),
    )
)
