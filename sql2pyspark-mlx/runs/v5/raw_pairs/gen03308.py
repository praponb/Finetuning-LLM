from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("effective_from", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2)).alias("net_prices"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "effective_from",
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
