from pyspark.sql import functions as F

result = df_shipments.withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
