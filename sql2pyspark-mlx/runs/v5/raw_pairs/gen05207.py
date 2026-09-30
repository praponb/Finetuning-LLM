from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("product_id").desc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("position"),
        F.col("li_item.product_id"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "product_id",
        "qty",
        "price",
        F.cume_dist().over(w).alias("product_id_cume_dist"),
        F.sum("product_id").over(w2).alias("product_id_sum_1h"),
    )
)
