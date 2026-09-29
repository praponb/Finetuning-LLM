from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_first())

latest = (
    df_store_sales
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select("id", "store_id", "product_id", "units", "revenue")
    .orderBy(F.col("store_id").asc_nulls_first(), "units")
)
