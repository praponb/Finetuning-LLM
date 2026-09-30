from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("entry_price").desc(), "id")
w_recent = Window.partitionBy("currency")
w3 = Window.orderBy("entry_price", "id")
w4 = Window.partitionBy("plan").orderBy("entry_price", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("currency").orderBy("entry_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_limits",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.row_number().over(w).alias("seq_no"),
        F.max("entry_price").over(w_recent).alias("currency_max_entry_price"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("entry_price").over(w4).alias("entry_price_avg_5_prior"),
        F.last("entry_price", ignorenulls=True).over(w5).alias("last_known_entry_price"),
    )
)
