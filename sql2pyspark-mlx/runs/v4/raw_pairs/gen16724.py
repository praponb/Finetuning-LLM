from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("is_usd").asc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("is_usd").asc_nulls_first())
w_running = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "currency", F.explode_outer("discounts").alias("element"))
)

filtered = (
    lines
    .filter(F.col("currency").isin(["THB", "USD"]))
    .withColumn("element_adjusted", F.round((F.col("element") * 100), 2))
)

cleaned = (
    filtered
    .filter(F.col("currency") == "THB")
    .withColumn("is_usd", F.when(F.col("currency") == "USD", 1).otherwise(0))
)

result = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("is_usd_percent_rank"),
        F.cume_dist().over(w2).alias("is_usd_cume_dist"),
        F.min("is_usd").over(w_running).alias("currency_min_is_usd"),
        F.min("element_adjusted").over(w_running).alias("currency_min_element_adjusted"),
    )
)
