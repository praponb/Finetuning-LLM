from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sorted_coupon_codes_count")
w2 = Window.orderBy(F.col("sorted_coupon_codes_count").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "coupon_codes",
        "value",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes"),
    )
)

valid = (
    cleaned
    .filter(F.size("sorted_coupon_codes") > 1)
    .select("*", F.array_join("coupon_codes", ", ").alias("coupon_codes_text"))
)

base = (
    valid
    .filter(F.size("coupon_codes") > 1)
    .select(
        "*",
        F.array_join("sorted_coupon_codes", "|").alias("sorted_coupon_codes_text"),
        F.size("sorted_coupon_codes").alias("sorted_coupon_codes_count"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("sorted_coupon_codes_count_percent_rank"),
        F.dense_rank().over(w).alias("sorted_coupon_codes_count_dense_rank"),
        F.rank().over(w2).alias("sorted_coupon_codes_count_rank"),
    )
)

result = (
    enriched
    .select(
        "coupon_codes",
        "value",
        "sorted_coupon_codes",
        "coupon_codes_text",
        "sorted_coupon_codes_text",
        "sorted_coupon_codes_count",
        "sorted_coupon_codes_count_dense_rank",
        "sorted_coupon_codes_count_rank",
    )
)
