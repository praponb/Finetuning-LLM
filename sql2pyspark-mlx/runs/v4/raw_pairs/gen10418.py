from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id")
w2 = Window.partitionBy("customer_id").orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes").getItem("device").alias("device"),
        F.size("items").alias("num_items"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.ntile(3).over(w).alias("num_items_bucket"),
        F.sum("num_items").over(w2).alias("running_num_items"),
        F.avg("num_items").over(w3).alias("num_items_avg_1h"),
    )
)
