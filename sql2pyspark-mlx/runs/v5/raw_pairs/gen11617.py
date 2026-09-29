from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy("entry_price", "id")
w4 = Window.partitionBy("plan").orderBy(F.col("effective_from").desc(), "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_limits",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "entry_price",
        F.sum("entry_price").over(w).alias("running_entry_price"),
        F.min("entry_price").over(w2).alias("plan_min_entry_price"),
        F.lag("entry_price", 1).over(w3).alias("prev_entry_price"),
        F.row_number().over(w4).alias("row_seq"),
    )
)
