from pyspark.sql import functions as F

result = df_price_tiers.select("id", F.explode_outer("discounts").alias("element"))
