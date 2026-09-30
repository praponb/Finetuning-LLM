from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")

result = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "carrier", "cost", "delivered_date")
    .orderBy("carrier", F.col("delivered_date").desc())
)
