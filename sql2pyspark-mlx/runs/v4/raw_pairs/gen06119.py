from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cart_value", "id")
w_grp = Window.orderBy("bulk_lines", "id")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

parsed = (
    df_carts
    .filter(F.col("created_at") < "2023-05-01")
    .select(
        "id",
        "created_at",
        "ship_to",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(10).over(w_grp).alias("bulk_lines_bucket"),
        F.lag("cart_value", 1, 0).over(w_grp).alias("prev_cart_value"),
        F.sum("bulk_lines").over(w3).alias("bulk_lines_sum_30m"),
    )
)
