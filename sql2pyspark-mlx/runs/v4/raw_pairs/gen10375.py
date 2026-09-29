from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.orderBy("discount", "id")
w3 = Window.partitionBy("status").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w4 = Window.partitionBy("status", "product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id", "status")
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("status", "product_id").orderBy("price_times_discount", "id")
w8 = Window.partitionBy("product_id").orderBy("price_times_discount", "id")
w9 = Window.partitionBy("status").orderBy(F.col("price_times_discount_bucket").desc())
w10 = Window.partitionBy("product_id").orderBy(F.col("unit_price_bucket").desc(), "id")
w_hist = Window.orderBy(F.col("rank_in_group_2").desc(), "id")
w12 = Window.partitionBy("status").orderBy(F.col("quantity").desc(), "id")

enriched = (
    df_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select("s.*", F.col("o.discount"), F.col("o.status"))
)

with_windows = (
    enriched
    .withColumn("unit_price_bucket", F.ntile(5).over(w))
    .withColumn("discount_bucket", F.ntile(3).over(w2))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.product_name"), F.col("p.price"))
)

cleaned = (
    with_dims
    .filter(F.lower(F.trim("product_name")) == "yoga mat")
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        "status",
        "unit_price_bucket",
        "rank_in_group",
        "product_name",
        (F.col("price") * F.col("discount")).alias("price_times_discount"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(3).over(w4).alias("quantity_bucket"),
        F.avg("price_times_discount").over(w5).alias("product_id_avg_price_times_discount"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w6)), 4).alias("unit_price_share"),
        F.ntile(3).over(w7).alias("price_times_discount_bucket"),
        F.ntile(10).over(w8).alias("price_times_discount_bucket_2"),
    )
)

latest_order_items = (
    scored
    .withColumn("rank_in_group_2", F.rank().over(w9))
    .filter(F.col("rank_in_group_2") == 1)
)

result = (
    latest_order_items
    .withColumn("unit_price_bucket_bucket", F.ntile(4).over(w10))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("quantity_bucket_2", F.ntile(10).over(w12))
)
