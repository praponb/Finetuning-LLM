from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_city").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("max_item_price", "id")

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("ship_to.city").alias("ship_city"),
        F.transform(
            F.filter("items", lambda x: x["promo"].isNotNull()),
            lambda x: F.struct(x["product_id"].alias("pid"), F.round(x["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)

result = (
    unpacked
    .withColumn("last_known_ship_city", F.last("ship_city", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w_part))
)
