from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("max_item_price").asc_nulls_last())
w2 = Window.orderBy("max_item_price", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("weight_kg").desc())
w_ordered = Window.orderBy(F.col("weight_kg").desc_nulls_first())
w5 = Window.orderBy("max_item_price", "id")

flattened_attrs = (
    df_carts
    .filter(F.size("coupon_codes") >= 2)
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.dense_rank().over(w).alias("max_item_price_dense_rank"),
        F.avg("weight_kg").over(w2).alias("weight_kg_avg_6"),
        F.rank().over(w3).alias("weight_kg_rank"),
        F.cume_dist().over(w_ordered).alias("weight_kg_cume_dist"),
        F.lag("max_item_price", 1).over(w5).alias("prev_max_item_price"),
    )
)
