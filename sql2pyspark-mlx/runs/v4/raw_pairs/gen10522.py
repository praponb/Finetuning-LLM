from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("max_item_price")
w2 = Window.partitionBy("customer_id").orderBy("max_item_price", "id")
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy("max_item_price", "id")

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "attributes",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
    )
)

result = (
    unpacked
    .withColumn("max_item_price_cume_dist", F.cume_dist().over(w))
    .withColumn("next_max_item_price", F.lead("max_item_price", 1).over(w2))
    .withColumn("max_item_price_share", F.round((F.col("max_item_price") / F.sum("max_item_price").over(w3)), 4))
    .withColumn("customer_id_count_max_item_price", F.count("max_item_price").over(w3))
    .withColumn("prev_max_item_price", F.lag("max_item_price", 2).over(w4))
)
