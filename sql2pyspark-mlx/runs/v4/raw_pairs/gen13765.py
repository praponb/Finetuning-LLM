from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.first("revenue", ignorenulls=True).over(w).alias("first_known_revenue"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.lag("revenue", 1).over(w3).alias("prev_revenue"),
    )
)
