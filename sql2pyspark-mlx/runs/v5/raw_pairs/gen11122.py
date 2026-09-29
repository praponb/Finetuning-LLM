from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("num_items").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("num_attributes", "id")
w3 = Window.partitionBy("customer_id").orderBy("created_at", "id")
w_part = Window.partitionBy("customer_id")
w_running = Window.partitionBy("customer_id").orderBy("num_items", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("attributes").alias("num_attributes"),
        F.size("items").alias("num_items"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        F.ntile(10).over(w).alias("num_items_bucket"),
        F.lead("num_items", 1).over(w2).alias("next_num_items"),
        F.lag("num_items", 1).over(w3).alias("prev_num_items"),
        F.count("num_attributes").over(w_part).alias("customer_id_count_num_attributes"),
        F.ntile(10).over(w_running).alias("num_items_bucket_2"),
    )
)
