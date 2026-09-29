from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")

result = (
    df_shipments
    .select("id", "carrier", "delivered_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .select("id", "carrier", "delivered_date")
    .orderBy("delivered_date")
)
