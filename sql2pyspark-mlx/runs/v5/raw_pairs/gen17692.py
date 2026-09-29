from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy(F.col("effective_from").desc(), "id")
w2 = Window.orderBy("effective_from", "id")

flattened_attrs = (
    df_price_tiers
    .filter(F.col("effective_from") < "2025-10-01")
    .select(
        "id",
        "effective_from",
        "tier_limits",
        "tier_prices",
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        F.row_number().over(w_part).alias("row_seq"),
        F.row_number().over(w_part).alias("row_seq_2"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq_3"),
    )
)
