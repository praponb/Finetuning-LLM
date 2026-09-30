from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")

result = (
    df_shipments
    .filter(F.col("cost").between(27, 58))
    .select("id", "order_id", "cost", F.min("cost").over(w).alias("order_id_min_cost"))
)
