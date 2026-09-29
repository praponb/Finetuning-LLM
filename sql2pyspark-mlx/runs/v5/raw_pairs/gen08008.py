from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id")
w2 = Window.orderBy("num_attributes", "id").rowsBetween(-6, -1)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.size("attributes").alias("num_attributes"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "gift_or_default",
        F.lag("items_total", 1).over(w).alias("prev_items_total"),
        F.avg("num_attributes").over(w2).alias("num_attributes_avg_6_prior"),
    )
)
