from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("cost", "id")

result = df_shipments.select("id", "carrier", "cost", F.ntile(4).over(w).alias("quartile"))
