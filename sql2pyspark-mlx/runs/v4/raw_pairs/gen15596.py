from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "brand", "launch_date")
    .orderBy("brand")
)
