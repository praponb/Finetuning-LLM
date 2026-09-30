from pyspark.sql import functions as F

result = df_shipments.select("id", F.datediff("delivered_date", "shipped_date").alias("transit_days"))
