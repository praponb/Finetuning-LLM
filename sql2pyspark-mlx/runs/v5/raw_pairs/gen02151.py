from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("element").orderBy(F.col("qty_str").desc_nulls_last())
w2 = Window.partitionBy("element")
w_ordered = Window.orderBy(F.col("product_id").desc())

lines = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

exploded = lines.select("product_id", "qty", F.explode("coupon_codes").alias("element"))

valid = (
    exploded
    .filter(F.col("element").isNotNull())
    .select("*", F.col("product_id").cast("double").alias("product_id_num"))
)

cleaned = (
    valid
    .filter(F.col("qty") < 4)
    .select("*", F.col("qty").cast("double").alias("qty_str"))
)

with_windows = (
    cleaned
    .select(
        "product_id",
        "qty_str",
        F.rank().over(w_recent).alias("qty_str_rank"),
        F.max("qty").over(w2).alias("element_max_qty"),
        F.percent_rank().over(w_ordered).alias("product_id_percent_rank"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("product_id") >= 10, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
    )
    .agg(
        F.round(F.avg("qty_str_rank"), 2).alias("avg_qty_str_rank"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.percentile_approx("product_id_percent_rank", 0.5).alias("product_id_percent_rank_p50"),
    )
)

result = (
    daily
    .select(
        "product_id_tier",
        "avg_qty_str_rank",
        "avg_product_id",
        "product_id_percent_rank_p50",
    )
    .orderBy(F.col("avg_qty_str_rank").desc_nulls_first())
)
