from pyspark.sql import functions as F

result = df_suppliers.withColumn("rating_scaled", F.round((F.col("rating") * 0.9), 2))
