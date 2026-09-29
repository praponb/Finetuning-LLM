from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w_ordered = Window.orderBy(F.col("price").desc())

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", F.col("elem.product_id"), F.col("elem.price"), F.col("elem.promo"))
)

result = (
    flattened
    .filter(F.col("promo") == "BUNDLE")
    .select(
        "id",
        "product_id",
        "promo",
        F.avg("product_id").over(w).alias("promo_avg_product_id"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share_2"),
        F.dense_rank().over(w_ordered).alias("price_dense_rank"),
    )
)
