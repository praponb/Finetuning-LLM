from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = exploded.select("effective_from", "currency")
