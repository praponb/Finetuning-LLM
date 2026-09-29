from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.rank().over(w).alias("revenue_rank"),
        F.first("units", ignorenulls=True).over(w2).alias("first_known_units"),
        F.first("revenue", ignorenulls=True).over(w3).alias("first_known_revenue"),
        F.avg("revenue").over(w4).alias("revenue_avg_3"),
        F.sum("revenue").over(w5).alias("running_revenue"),
    )
)
