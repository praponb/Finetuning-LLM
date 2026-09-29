from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .withColumn("avg_revenue_by_product", F.avg("revenue").over(w))
    .filter(F.col("revenue") < F.col("avg_revenue_by_product"))
    .select("id", "product_id", "revenue", "sale_date")
)
