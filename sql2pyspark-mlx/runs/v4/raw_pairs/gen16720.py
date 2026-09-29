from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy("tier_value")
w_ordered = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    flattened
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_ordered))
)
