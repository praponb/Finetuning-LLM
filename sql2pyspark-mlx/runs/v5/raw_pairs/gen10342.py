from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    parsed
    .select(
        "id",
        "created_at",
        "city_or_unknown",
        F.first("utm_source", ignorenulls=True).over(w_hist).alias("first_known_utm_source"),
        F.first("gift_or_default", ignorenulls=True).over(w_hist).alias("first_known_gift_or_default"),
        F.first("city_or_unknown", ignorenulls=True).over(w_hist).alias("first_known_city_or_unknown"),
    )
)
