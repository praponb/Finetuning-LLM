from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipped_date", "id")

result = (
    df_shipments
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "status", "shipped_date", "order_id")
    .orderBy(F.col("order_id").desc())
)
