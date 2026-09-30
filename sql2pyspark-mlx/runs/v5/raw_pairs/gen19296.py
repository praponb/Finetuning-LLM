from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("price").desc_nulls_first())

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

result = (
    lines
    .select(
        "customer_id",
        "created_at",
        "price",
        F.sum("product_id").over(w).alias("customer_id_sum_product_id"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
    )
)
