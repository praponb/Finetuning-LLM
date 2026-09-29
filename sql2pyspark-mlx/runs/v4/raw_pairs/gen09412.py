from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())

latest = (
    df_inventory
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "store_id", "last_restock", "row_num")
    .orderBy(F.col("last_restock").desc(), F.col("id").desc())
)
