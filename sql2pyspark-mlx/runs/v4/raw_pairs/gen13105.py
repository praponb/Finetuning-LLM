from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("lowest_price").desc(), "id")
w3 = Window.partitionBy("plan").orderBy("lowest_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("plan").orderBy("lowest_price", "id")

unpacked = (
    df_price_tiers
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.sum("entry_price").over(w).alias("running_entry_price"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("lowest_price").over(w3).alias("cumulative_lowest_price"),
        F.first("entry_price", ignorenulls=True).over(w).alias("first_known_entry_price"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)
