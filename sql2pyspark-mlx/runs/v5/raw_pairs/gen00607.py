from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("lowest_price", "id")
w2 = Window.orderBy(F.col("lowest_price").asc_nulls_first())
w3 = Window.orderBy(F.col("lowest_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("lowest_price") < 25)
    .withColumn("lowest_price_bucket", F.ntile(3).over(w_recent))
    .withColumn("next_lowest_price", F.lead("lowest_price", 1, 0).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("lowest_price_cume_dist", F.cume_dist().over(w2))
    .withColumn("lowest_price_bucket_2", F.ntile(10).over(w3))
)
