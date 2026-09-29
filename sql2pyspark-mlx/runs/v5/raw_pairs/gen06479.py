from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("thb_rows").desc())

parsed = (
    df_price_tiers
    .filter(F.col("plan") == "enterprise")
    .select(
        "id",
        "plan",
        "currency",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    parsed
    .groupBy("currency", "plan")
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
    )
    .withColumn("thb_rows_dense_rank", F.dense_rank().over(w))
)
