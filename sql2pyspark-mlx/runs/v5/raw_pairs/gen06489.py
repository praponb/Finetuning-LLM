from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
)
