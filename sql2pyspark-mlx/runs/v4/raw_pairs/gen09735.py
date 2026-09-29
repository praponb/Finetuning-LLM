from pyspark.sql import functions as F

exploded = df_price_tiers.select("id", "currency", F.explode("tier_limits").alias("val"))

result = exploded.select("id", "currency", "val")
