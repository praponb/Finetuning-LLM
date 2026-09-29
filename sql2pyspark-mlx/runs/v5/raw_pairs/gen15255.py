from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("id", "status", F.count("*").over(w).alias("status_count"))
)
