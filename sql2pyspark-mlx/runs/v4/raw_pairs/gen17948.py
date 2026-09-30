from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("avg_tier_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("plan").orderBy("avg_tier_price", "id")

parsed = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "tier_prices",
        "discounts",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2)).alias("avg_tier_price"),
    )
)

result = (
    parsed
    .withColumn("last_known_avg_tier_price", F.last("avg_tier_price", ignorenulls=True).over(w_grp))
    .withColumn("next_avg_tier_price", F.lead("avg_tier_price", 1).over(w_recent))
    .withColumn("seq_no", F.row_number().over(w_recent))
    .withColumn("last_known_avg_tier_price_2", F.last("avg_tier_price", ignorenulls=True).over(w_grp))
)
