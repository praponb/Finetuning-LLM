from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("plan").orderBy(F.col("lowest_price").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("avg_tier_price").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.array_min("tier_prices").alias("lowest_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.first("lowest_price", ignorenulls=True).over(w_hist).alias("first_known_lowest_price"),
        F.dense_rank().over(w_running).alias("lowest_price_dense_rank"),
        F.ntile(5).over(w3).alias("avg_tier_price_bucket"),
    )
)
