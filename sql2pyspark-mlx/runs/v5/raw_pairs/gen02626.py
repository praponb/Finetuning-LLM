from pyspark.sql import functions as F

result = df_stores.select("sqft", "store_name", "city")
