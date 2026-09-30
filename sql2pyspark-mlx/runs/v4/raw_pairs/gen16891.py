from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("entry_price", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("entry_price").desc(), "id")
w3 = Window.orderBy("entry_price", "id")

flattened_attrs = (
    df_price_tiers
    .select(
        "id",
        F.element_at("tier_prices", 1).alias("entry_price"),
        F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2)).alias("tier_revenue_caps"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.avg("entry_price").over(w).alias("entry_price_avg_3_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(5).over(w3).alias("entry_price_bucket"),
    )
)
