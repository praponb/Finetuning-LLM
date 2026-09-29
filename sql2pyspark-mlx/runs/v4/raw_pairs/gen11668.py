from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("price").desc())
w3 = Window.partitionBy("customer_id").orderBy(F.col("price").desc_nulls_last())

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
    )
)

result = (
    flattened
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w)), 4))
    .withColumn("price_rank", F.rank().over(w2))
    .withColumn("price_percent_rank", F.percent_rank().over(w3))
)
