from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy("revenue", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .withColumn("revenue_rank", F.rank().over(w))
    .withColumn("revenue_bucket", F.ntile(10).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
