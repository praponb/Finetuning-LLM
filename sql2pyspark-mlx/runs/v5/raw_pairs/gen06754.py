from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "product_id", "revenue", "sale_date")
    .orderBy(F.col("revenue").desc())
)
