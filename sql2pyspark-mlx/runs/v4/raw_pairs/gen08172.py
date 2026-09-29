from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .withColumn("max_cost_by_status", F.max("cost").over(w))
    .filter(F.col("cost") == F.col("max_cost_by_status"))
    .select("id", "status", "cost", "shipped_date")
)
