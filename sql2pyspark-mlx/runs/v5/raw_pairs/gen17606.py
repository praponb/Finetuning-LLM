from pyspark.sql import functions as F

result = df_stores.agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
