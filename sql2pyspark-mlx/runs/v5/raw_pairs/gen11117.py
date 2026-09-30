from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("max_item_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "ship_country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.forall("items", lambda x: x["qty"] <= 5).alias("all_small_qty"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "customer_id",
        "ship_country",
        "all_small_qty",
        F.count("max_item_price").over(w_recent).alias("running_count_max_item_price"),
        F.sum("max_item_price").over(w_recent).alias("running_sum_max_item_price"),
        F.max("max_item_price").over(w_recent).alias("running_max_max_item_price"),
        F.count("max_item_price").over(w2).alias("max_item_price_count_30m"),
    )
)
