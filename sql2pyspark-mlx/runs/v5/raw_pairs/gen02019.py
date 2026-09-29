from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        F.last("entry_price", ignorenulls=True).over(w).alias("last_known_entry_price"),
        F.count("avg_tier_price").over(w2).alias("plan_count_avg_tier_price"),
    )
)
