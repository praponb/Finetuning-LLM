from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("tier_value").desc_nulls_last())

flattened = df_price_tiers.select("id", F.explode_outer("discounts").alias("tier_value"))

result = (
    flattened
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("tier_value_percent_rank_2", F.percent_rank().over(w_recent))
)
