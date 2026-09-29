from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("delivered_date").desc())

result = (
    df_shipments
    .select("id", "status", "delivered_date", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "delivered_date")
)
