from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id").rowsBetween(-6, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("items").alias("num_items"),
        F.col("attributes").getItem("gift").alias("gift"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "num_items",
        "gift",
        F.sum("num_items").over(w).alias("last7_sum_num_items"),
        F.avg("num_items").over(w).alias("last7_avg_num_items"),
        F.min("num_items").over(w).alias("last7_min_num_items"),
    )
)
