from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id")
w_recent = Window.orderBy(F.col("sale_date").desc(), "id")
w4 = Window.orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.ntile(4).over(w).alias("units_bucket"),
        F.corr("units", "revenue").over(w2).alias("corr_units_revenue"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("revenue").over(w4).alias("revenue_avg_3"),
    )
)
