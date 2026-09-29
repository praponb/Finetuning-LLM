from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cart_value").asc_nulls_first())
w_recent = Window.orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("cart_value").desc(), "id")
w_recent_2 = Window.orderBy(F.col("cart_value").desc_nulls_first())

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.size("items").alias("num_items"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
    )
)

result = (
    flattened_attrs
    .withColumn("cart_value_dense_rank", F.dense_rank().over(w))
    .withColumn("last_known_max_item_price", F.last("max_item_price", ignorenulls=True).over(w_recent))
    .withColumn("last_known_max_item_price_2", F.last("max_item_price", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("cart_value_percent_rank", F.percent_rank().over(w_recent_2))
)
