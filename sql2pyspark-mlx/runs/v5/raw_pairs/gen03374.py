from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("bulk_lines").asc_nulls_first())
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.orderBy("bulk_lines", "id")

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    parsed
    .filter(F.col("bulk_lines").between(1, 3))
    .select(
        "id",
        "created_at",
        "has_welcome_code",
        "bulk_lines",
        F.percent_rank().over(w).alias("bulk_lines_percent_rank"),
        F.avg("bulk_lines").over(w2).alias("bulk_lines_avg_30m"),
        F.lag("bulk_lines", 1).over(w3).alias("prev_bulk_lines"),
    )
)
