from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("effective_from", "id")
w2 = Window.partitionBy("currency").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("plan").orderBy("effective_from", "id")
w4 = Window.partitionBy("currency").orderBy("lowest_price", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("lowest_price", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        "discounts",
        F.array_min("tier_prices").alias("lowest_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.lead("lowest_price", 2, 0).over(w).alias("next_lowest_price"),
        F.sum("lowest_price").over(w2).alias("running_lowest_price"),
        F.row_number().over(w_part).alias("seq_no"),
        F.avg("lowest_price").over(w4).alias("lowest_price_avg_3"),
        F.lag("lowest_price", 2).over(w5).alias("prev_lowest_price"),
    )
)
