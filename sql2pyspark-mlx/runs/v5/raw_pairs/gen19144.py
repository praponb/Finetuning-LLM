from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_city").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("created_at", "id").rowsBetween(-2, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.size("items").alias("num_items"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.city").alias("ship_city"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    flattened_attrs
    .withColumn("first_known_num_items", F.first("num_items", ignorenulls=True).over(w))
    .withColumn("last_known_num_items", F.last("num_items", ignorenulls=True).over(w2))
    .withColumn("last_known_num_items_2", F.last("num_items", ignorenulls=True).over(w3))
    .withColumn("num_items_avg_2", F.avg("num_items").over(w4))
)
