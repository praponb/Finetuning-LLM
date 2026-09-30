from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 6)
    .withColumn("revenue_bucket", F.ntile(4).over(w))
    .withColumn("units_avg_2", F.avg("units").over(w2))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w3))
)
