from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("qty").desc_nulls_last())
w_running = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_first())
w3 = Window.orderBy(F.col("price").desc_nulls_last())
w4 = Window.partitionBy("promo", "customer_id")

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .select(
        "id",
        "coupon_codes",
        "product_id",
        "price",
        F.percent_rank().over(w).alias("qty_percent_rank"),
        F.dense_rank().over(w_running).alias("qty_dense_rank"),
        F.rank().over(w3).alias("price_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w4)), 4).alias("product_id_share"),
    )
)
