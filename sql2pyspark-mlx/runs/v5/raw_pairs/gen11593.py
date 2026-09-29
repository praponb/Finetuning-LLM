from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "quantity", "last_restock")
    .orderBy(F.col("quantity").desc(), F.col("id").desc())
)
