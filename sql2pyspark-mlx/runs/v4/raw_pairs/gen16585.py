from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("lowest_price").desc(), "id")
w2 = Window.partitionBy("currency").orderBy(F.col("effective_from").desc(), "id")
w3 = Window.partitionBy("currency").orderBy("entry_price", "id")

parsed = (
    df_price_tiers
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "effective_from",
        "currency",
        "tier_limits",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)
