from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("qty").asc_nulls_last())
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("customer_id").orderBy(F.col("qty").desc())

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .withColumn("qty_cume_dist", F.cume_dist().over(w))
    .withColumn("promo_min_qty", F.min("qty").over(w2))
    .withColumn("qty_cume_dist_2", F.cume_dist().over(w3))
    .withColumn("promo_stddev_samp_qty", F.stddev_samp("qty").over(w2))
)
