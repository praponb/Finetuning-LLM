from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("created_at", "id")

parsed = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("attributes")["gift"].alias("gift"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "basket_amount",
        "all_small_qty",
        F.lead("basket_amount").over(w_ordered).alias("next_basket_amount"),
        F.lag("basket_amount").over(w_ordered).alias("prev_basket_amount"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)
