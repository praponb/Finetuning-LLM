from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "carrier", "delivered_date")
    .orderBy(F.col("delivered_date").desc())
)
