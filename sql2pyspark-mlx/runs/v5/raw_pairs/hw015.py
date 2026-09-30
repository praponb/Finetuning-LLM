from pyspark.sql import functions as F

result = df_suppliers.select("id", F.pow("rating", 2).alias("rating_squared"))
