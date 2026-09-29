from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.avg("revenue").over(w_ordered).alias("revenue_avg_3"),
        F.lag("units", 1).over(w2).alias("prev_units"),
    )
)
