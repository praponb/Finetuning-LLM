from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "store_id", "revenue", "units")
    .orderBy(F.col("revenue").desc())
)
