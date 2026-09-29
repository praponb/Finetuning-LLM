from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").desc(), "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "coupon_codes",
        "has_welcome_code",
        "used_welcome5",
        "utm_source_or_default",
        F.row_number().over(w).alias("seq_no"),
    )
)
