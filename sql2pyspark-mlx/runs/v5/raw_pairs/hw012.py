from pyspark.sql import functions as F

result = df_suppliers.select("id", F.round("rating", 1).alias("rounded_rating"))
