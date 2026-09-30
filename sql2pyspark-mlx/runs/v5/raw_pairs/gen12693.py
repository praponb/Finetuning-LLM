from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("element"))

result = lines.select("id", "plan", "element")
