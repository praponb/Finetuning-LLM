from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "store_id", "revenue")
    .orderBy(F.col("id").desc())
)
