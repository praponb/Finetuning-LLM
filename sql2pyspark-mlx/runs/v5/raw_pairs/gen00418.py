from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("product_id").desc_nulls_first())
w_ordered = Window.orderBy("product_id")

exploded = (
    df_carts
    .withColumn("li_item", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
    )
)

filtered = (
    exploded
    .filter(F.col("created_at") < "2025-04-01")
    .select(
        "coupon_codes",
        "product_id",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
    )
)

filtered_carts = (
    filtered
    .filter(F.col("product_id").between(5, 9))
    .withColumn("is_bundle", F.array_contains("coupon_codes", "BUNDLE"))
)

filtered_carts_2 = (
    filtered_carts
    .filter(F.col("product_id") >= 9)
    .select("*", F.array_join("coupon_codes", "|").alias("coupon_codes_text_2"))
)

result = (
    filtered_carts_2
    .filter(~F.col("is_bundle"))
    .select(
        "coupon_codes",
        "product_id",
        "coupon_codes_text",
        "is_bundle",
        "coupon_codes_text_2",
        F.dense_rank().over(w).alias("product_id_dense_rank"),
        F.percent_rank().over(w_ordered).alias("product_id_percent_rank"),
    )
)
