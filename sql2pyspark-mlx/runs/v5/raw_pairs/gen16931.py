from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("cart_value").desc(), "id")
w2 = Window.orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.size("items").alias("num_items"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "num_items",
        "max_item_price",
        "zip_or_unknown",
        "has_premium_item",
        "cart_value",
        F.ntile(5).over(w).alias("cart_value_bucket"),
        F.sum("cart_value").over(w2).alias("running_cart_value"),
    )
)
