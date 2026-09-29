from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")

result = (
    df_shipments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "carrier", "cost", "delivered_date")
    .orderBy(F.col("id").desc())
)
