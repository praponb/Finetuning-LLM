from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        F.avg("revenue").over(w_recent).alias("revenue_avg_2"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.ntile(5).over(w3).alias("revenue_bucket"),
    )
)
