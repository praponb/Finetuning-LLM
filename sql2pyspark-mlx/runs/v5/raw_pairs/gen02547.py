from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("promo", "customer_id")
w3 = Window.partitionBy("promo", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    exploded
    .select(
        "created_at",
        "coupon_codes",
        "product_id",
        "promo",
        F.count("price").over(w).alias("customer_id_count_price"),
        F.avg("qty").over(w2).alias("promo_avg_qty"),
        F.sum("product_id").over(w3).alias("product_id_sum_2h"),
        F.sum("product_id").over(w4).alias("product_id_sum_2h_2"),
    )
)
