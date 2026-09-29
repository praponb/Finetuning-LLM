from pyspark.sql import functions as F

result = df_carts.select("id", F.explode_outer("attributes").alias("attr_key", "attr_value"))
