from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date")

result = (
    df_shipments
    .select("id", "order_id", "delivered_date", F.rank().over(w).alias("rnk"))
)
