from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("basket_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("basket_amount", "id")
w3 = Window.orderBy("basket_amount", "id").rowsBetween(-6, -1)

parsed = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("direct")).alias("gift_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.last("utm_source_or_default", ignorenulls=True).over(w).alias("last_known_utm_source_or_default"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("basket_amount").over(w3).alias("basket_amount_avg_6_prior"),
    )
)
