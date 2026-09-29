from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("units").between(14, 35))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") == 1)
    .orderBy(F.col("id").desc())
)
