from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

latest = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "store_id", "row_num")
    .orderBy(F.col("id").desc(), "row_num")
)
