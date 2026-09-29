from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

result = (
    exploded
    .withColumn("product_id_share", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("product_id_share_2", F.round((F.col("product_id") / F.sum("product_id").over(w)), 4))
    .withColumn("price_count_30m", F.count("price").over(w2))
)
