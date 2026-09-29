from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, -1)

result = (
    df_store_sales
    .select(
        "*",
        F.max("revenue").over(w).alias("store_id_max_revenue"),
        F.first("units", ignorenulls=True).over(w2).alias("first_known_units"),
        F.avg("revenue").over(w3).alias("revenue_avg_6"),
        F.sum("revenue").over(w).alias("store_id_sum_revenue"),
        F.avg("units").over(w4).alias("units_avg_6_prior"),
    )
)
