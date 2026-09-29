from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")

result = (
    df_shipments
    .filter(F.col("carrier") != "UPS")
    .select("id", "order_id", "delivered_date", F.row_number().over(w).alias("rn"))
    .orderBy("delivered_date", F.col("id").desc())
)
