from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

result = (
    flattened
    .select(
        "customer_id",
        "coupon_codes",
        "product_id",
        "qty",
        F.percent_rank().over(w).alias("product_id_percent_rank"),
    )
)
