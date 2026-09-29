from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id")

result = (
    df_orders
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 2)
    .select("id", "status", "discount", "channel")
)
