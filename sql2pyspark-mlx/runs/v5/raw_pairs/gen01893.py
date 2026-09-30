from pyspark.sql import functions as F

result = df_stores.agg(F.round(F.avg("sqft"), 2).alias("avg_sqft"))
