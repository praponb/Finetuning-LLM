from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "promo").orderBy("pos", "id", "pos")
w2 = Window.partitionBy("promo").orderBy("created_at", "id", "pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    exploded
    .withColumn("prev_price", F.lag("price", 1).over(w))
    .withColumn("first_known_pos", F.first("pos", ignorenulls=True).over(w2))
)
