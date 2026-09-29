from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-05-01", "2023-05-02"))
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "carrier", "delivered_date")
    .orderBy(F.col("carrier").desc())
)
