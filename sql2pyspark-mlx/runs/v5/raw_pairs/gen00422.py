from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "store_id",
        "sale_date",
        "revenue",
        F.sum("units").over(w).alias("product_id_sum_units"),
        F.lag("revenue", 1, 0).over(w2).alias("prev_revenue"),
    )
    .orderBy("sale_date")
)
