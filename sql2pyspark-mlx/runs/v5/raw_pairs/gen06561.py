from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .select("id", "status", "cost", F.min("cost").over(w).alias("status_min_cost"))
)
