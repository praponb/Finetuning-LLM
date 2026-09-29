from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")

result = (
    df_shipments
    .select("id", "carrier", "cost", F.min("cost").over(w).alias("carrier_min_cost"))
    .orderBy("cost")
)
