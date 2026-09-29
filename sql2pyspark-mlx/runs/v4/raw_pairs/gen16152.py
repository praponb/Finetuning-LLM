from pyspark.sql import functions as F

result = df_stores.agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
