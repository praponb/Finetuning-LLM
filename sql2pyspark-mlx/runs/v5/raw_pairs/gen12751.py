from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan").orderBy(F.col("effective_from").desc(), "id")
w2 = Window.orderBy("lowest_price", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("entry_price").desc_nulls_last())

unpacked = (
    df_price_tiers
    .filter(F.col("plan").isin(["enterprise", "starter"]))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.element_at("tier_prices", 1).alias("entry_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        "effective_from",
        "avg_tier_price",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(3).over(w2).alias("lowest_price_bucket"),
        F.percent_rank().over(w3).alias("entry_price_percent_rank"),
    )
)
