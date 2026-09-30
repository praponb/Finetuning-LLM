from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w2 = Window.partitionBy("promo")

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.promo"),
    )
)

result = (
    flattened
    .select(
        "id",
        "created_at",
        F.percent_rank().over(w).alias("product_id_percent_rank"),
        F.count("product_id").over(w2).alias("promo_count_product_id"),
    )
)
