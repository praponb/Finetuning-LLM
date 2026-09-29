from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("weight_kg", "id")
w2 = Window.orderBy("num_items", "id").rowsBetween(-5, Window.currentRow)
w_ordered = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
    )
)

result = (
    parsed
    .withColumn("prev_weight_kg", F.lag("weight_kg", 2, 0).over(w_grp))
    .withColumn("num_items_avg_5", F.avg("num_items").over(w2))
    .withColumn("running_num_items", F.sum("num_items").over(w_ordered))
    .withColumn("weight_kg_bucket", F.ntile(5).over(w_grp))
    .withColumn("last_known_gift_or_default", F.last("gift_or_default", ignorenulls=True).over(w4))
)
