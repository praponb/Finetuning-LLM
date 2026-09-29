from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("entry_price", "id")
w2 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_prices",
        "discounts",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .filter(F.col("effective_from") < "2023-07-01")
    .select(
        "id",
        "effective_from",
        F.row_number().over(w).alias("event_seq"),
        F.last("entry_price", ignorenulls=True).over(w2).alias("last_known_entry_price"),
    )
)
