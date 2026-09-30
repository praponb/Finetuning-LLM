from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("store_id").orderBy("revenue", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "store_id",
        F.avg("revenue").over(w).alias("revenue_avg_6_prior"),
        F.ntile(10).over(w2).alias("revenue_bucket"),
        F.avg("units").over(w3).alias("units_avg_2"),
        F.corr("units", "revenue").over(w4).alias("corr_units_revenue"),
    )
)
