from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("revenue").between(1600, 3000))
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "product_id", "sale_date", "revenue")
)
