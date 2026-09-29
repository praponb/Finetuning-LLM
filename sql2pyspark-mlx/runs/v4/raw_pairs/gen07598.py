from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue")

result = (
    df_store_sales
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "product_id", "revenue", "store_id")
    .orderBy(F.col("store_id").desc())
)
