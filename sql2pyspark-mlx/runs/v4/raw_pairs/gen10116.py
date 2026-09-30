from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("status", "carrier").orderBy(F.col("delivered_date").desc(), "id")

base = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .withColumn("carrier_lower", F.lower("carrier"))
)

result = (
    base
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
