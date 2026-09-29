from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").asc_nulls_first())

result = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
