from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .select("id", "plan", "currency", F.max("val").over(w).alias("currency_max_val"))
)
