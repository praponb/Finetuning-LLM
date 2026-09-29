from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("lowest_price").asc_nulls_first())

flattened_attrs = (
    df_price_tiers
    .filter(F.col("plan") != "starter")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "discounts",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.array_min("tier_prices").alias("lowest_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.first("lowest_price", ignorenulls=True).over(w).alias("first_known_lowest_price"),
        F.cume_dist().over(w2).alias("lowest_price_cume_dist"),
    )
)
