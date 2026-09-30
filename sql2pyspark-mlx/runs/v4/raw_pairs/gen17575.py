from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("lowest_price", "id")
w4 = Window.partitionBy("plan").orderBy(F.col("lowest_price").asc_nulls_first())

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "tier_prices",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .withColumn("lowest_price_avg_6_prior", F.avg("lowest_price").over(w))
    .withColumn("last_known_lowest_price", F.last("lowest_price", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("lowest_price_dense_rank", F.dense_rank().over(w4))
)
