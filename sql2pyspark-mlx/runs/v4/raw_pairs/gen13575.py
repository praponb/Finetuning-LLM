from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("effective_from", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    unpacked
    .withColumn("entry_price_avg_5_prior", F.avg("entry_price").over(w))
    .withColumn("entry_price_avg_6", F.avg("entry_price").over(w2))
)
